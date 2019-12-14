import hashlib
import logging
import random
import re
import subprocess
import tempfile
import time
from os.path import join

import requests
from bs4 import BeautifulSoup
from functional import seq
from requests import HTTPError


class IAPDSession(requests.Session):

    def __init__(self, min_delay_time=1.5, max_delay_time=2.5, timeout=60):
        super().__init__()
        self._min_delay_time = min_delay_time
        self._max_delay_time = max_delay_time
        self._timeout = timeout
        self._last_request_time = 0
        self._logger = logging.getLogger(self.__class__.__name__)

    def request(self, *args, **kwargs):
        kwargs['timeout'] = kwargs.get('timeout', self._timeout)
        self._delay_request_if_needed()
        self._logger.debug('%s with params %s' % (' '.join(args), kwargs))
        response = super().request(*args, **kwargs)
        self._last_request_time = time.time()
        response.raise_for_status()
        return response

    def _delay_request_if_needed(self):
        delay_time = random.uniform(self._min_delay_time, self._max_delay_time)
        if delay_time > 0 and self._last_request_time > 0:
            process_time = time.time() - self._last_request_time
            if process_time < delay_time:
                sleep_time = delay_time - process_time
                self._logger.debug('Request delayed for %ss.' % sleep_time)
                time.sleep(sleep_time)


class IAPDError(Exception):
    pass


class IAPD(object):

    HEADERS = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'max-age=0',
        'referer': 'https://adviserinfo.sec.gov/IAPD/default.aspx',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 '
                      'Safari/537.36',
    }

    BASE_URL = 'https://adviserinfo.sec.gov'
    DEFAULT_URL = 'https://adviserinfo.sec.gov/IAPD/default.aspx'
    SEARCH_URL = 'https://adviserinfo.sec.gov/IAPD/IAPDSearch.aspx'
    FIRM_URL = 'https://adviserinfo.sec.gov/Firm/{}'
    ADV_TWO_BROCHURE_BASE_URL = '/IAPD/Part2Brochures.aspx'
    INDIVIDUAL_URL = 'https://adviserinfo.sec.gov/Individual/{}'

    ADV_TWO_BROCHURE_ID = 'ctl00_cphMain_part2_dgBrchr_ctrl0_hlBrochureName'
    ADV_ONE_HREF_ID = 'ctl00_cphMain_landing_pdfLink'
    ADV_TWO_HREF_ID = 'ctl00_cphMain_landing_p2BrochureLink'
    NEXT_PAGE_ID = 'ctl00_cphMain_ucSearchPagerTop_pageNext'
    NEXT_PAGE_EVENT = 'ctl00$cphMain$ucSearchPagerTop$pageNext'
    COMPANY_NAME_ID = 'ctl00_cphMain_landing_lblActiveOrgName'
    DETAILED_REPORT_ID = 'ctl00_cphMain_btnGetReport'

    SEARCH_RESULT_ID_PATTERN = re.compile(r'ctl00_cphMain_rptrSearchResult_ctl\d{2,}_uc(Firm|Indvl)Item_hlSummary')
    CRD_PATTERN = re.compile(r'CRD# (\d+)')
    SEC_PATTERN = re.compile(r'SEC# ([\d-]+)')
    ADDRESS_ID_PATTERN = re.compile(r'ctl00_cphMain_rptrSearchResult_ctl\d{2,}_uc(Firm|Indvl)Item_divAddress')
    TYPE_PATTERN = re.compile(r'ctl00_cphMain_rptrSearchResult_ctl\d{2,}_uc(Firm|Indvl)Item_div\w{2,4}$')
    STATUS_PATTERN = re.compile(r'ctl00_cphMain_rptrSearchResult_ctl\d{2,}_uc(Firm|Indvl)Item_div\w{2,4}'
                                          r'(Inactive|NotLicensed)')
    SCOPES = {
        'individual': 'rdoIndvl',
        'firm': 'rdoFirm'
    }

    def __init__(self, min_delay_time=1.5, max_delay_time=2.5):
        self._session = IAPDSession(min_delay_time=min_delay_time, max_delay_time=max_delay_time)
        self._session.headers.update(self.HEADERS)
        self._data = {}
        self._logger = logging.getLogger(self.__class__.__name__)

    @staticmethod
    def _get_data(soup, term, scope, zip_code, zip_code_range, at_firm):
        view_state, view_state_generator, event_validation = map(
            lambda param: soup.find('input', attrs={'name': param}).get('value'),
            ['__VIEWSTATE', '__VIEWSTATEGENERATOR', '__EVENTVALIDATION'])

        data = {
                '__EVENTTARGET': 'ctl00$cphMain$sbox$searchBtn',
                '__VIEWSTATE': view_state,
                '__VIEWSTATEGENERATOR': view_state_generator,
                '__EVENTVALIDATION': event_validation,
                'ctl00$cphMain$sbox$searchScope': scope,
                'ctl00$cphMain$sbox$txt{}'.format(scope[3:]): term,
                'ctl00$cphMain$sbox$ddlZipRange': zip_code_range,
                'ctl00$cphMain$sbox$txtZip': zip_code,
                'ctl00$cphMain$sbox$txtAtFirm': at_firm
        }
        return data

    def _initialize_search(self, term, scope, zip_code, zip_code_range, at_firm):
        response = self._session.get(self.DEFAULT_URL)
        soup = BeautifulSoup(response.content, 'lxml')
        data = self._get_data(soup=soup,
                              term=term,
                              scope=scope,
                              zip_code=zip_code,
                              zip_code_range=zip_code_range,
                              at_firm=at_firm)
        self._session.post(self.DEFAULT_URL, data=data)

    def _get_adv_two_from_brochures_url(self, url):
        response = self._session.get(url)
        soup = BeautifulSoup(response.content, 'lxml')
        adv_two = (soup.find('a', attrs={'id': self.ADV_TWO_BROCHURE_ID}) or {}).get('href', "")
        return adv_two

    @staticmethod
    def _md5(text):
        return hashlib.md5(text.encode()).hexdigest()

    def _download_form(self, url, output_dir=None):
        folder = output_dir or tempfile.mkdtemp()
        local_path = join(folder, self._md5(url) + ".pdf")
        self._logger.debug('Downloaded file: {}'.format(local_path))
        try:
            r = self._session.get(url, allow_redirects=True)
            open(local_path, 'w').write(r.content)
        except HTTPError as e:
            self._logger.exception(e)
            if e.response.status_code == 502:
                self._logger.debug('Trying to use command to download.')
                command = "wget {} -O {}".format(url, local_path)
                subprocess.call(command, shell=True)
            else:
                raise IAPDError('Download failed: {}'.format(url))
        return local_path

    def search(self, term, scope='firm', zip_code=None, zip_code_range='5', at_firm=None,  iadp_only=False):
        """
        Search for firm or individual on https://adviserinfo.sec.gov
        Args:
            term: search term
            scope: search scope, can be either "firm" or "individual"
            zip_code: filter search result by zip code
            zip_code_range: specify a range from zip code (measured in miles)
            at_firm: used for individual search only to filter by current employer
            iadp_only: only return url from https://adviserinfo.sec.gov

        Returns:
            lists of results
        """
        if scope not in ['firm', 'individual']:
            raise IAPDError('Invalid search scope, must be firm or individual.')
        scope = self.SCOPES[scope]
        if not self._data:
            self._initialize_search(term=term,
                                    scope=scope,
                                    zip_code=zip_code,
                                    zip_code_range=zip_code_range,
                                    at_firm=at_firm)
            response = self._session.get(self.SEARCH_URL)
        else:
            self._data.update(
                {
                    'ctl00$cphMain$sbox$txt{}'.format(scope[3:]): term,
                    'ctl00$cphMain$sbox$searchScope': scope
                }
            )
            response = self._session.post(self.SEARCH_URL, data=self._data)
        next_page = True
        while next_page:
            soup = BeautifulSoup(response.content, 'lxml')
            self._data.update(self._get_data(soup=soup,
                                             term=term,
                                             scope=scope,
                                             zip_code=zip_code,
                                             zip_code_range=zip_code_range,
                                             at_firm=at_firm))
            next_page = soup.find('a', attrs={'id': self.NEXT_PAGE_ID})
            yield self._parse_search(soup=soup,
                                     iadp_only=iadp_only)
            if next_page:
                self._data.update({'__EVENTTARGET': self.NEXT_PAGE_EVENT})
                response = self._session.post(self.SEARCH_URL, data=self._data)

    def _parse_search(self, soup, iadp_only):
        results = soup.find_all('a', attrs={'class': 'alinkborder'})
        firms = []
        for result in results:
            url = result.get('href')

            display_card = result.find('span', attrs={'class': 'displaycrd'}).text
            crd_number = self.CRD_PATTERN.search(display_card)
            sec_number = self.SEC_PATTERN.search(display_card)

            alternate_names_element = result.find('span', attrs={'class': 'names'})
            alternate_names = alternate_names_element.text.strip() if alternate_names_element else None

            address_element = result.find('div', attrs={'id': self.ADDRESS_ID_PATTERN})
            firm_type_elements = result.find_all('div', attrs={'id': self.TYPE_PATTERN})
            firm_types = seq(firm_type_elements).map(lambda type_: dict(
                name=type_.find(text=True, recursive=False).strip(),
                status=0 if type_.find('div', attrs={'id': self.STATUS_PATTERN}) else 1
            )).list()
            firms.append(
                dict(
                    url=self.BASE_URL + url if url.startswith('/Firm') or url.startswith('/Individual') else url,
                    name=result.find('span', attrs={'class': 'displayname'}).text,
                    crd=crd_number.group(1) if crd_number else None,
                    sec=sec_number.group(1) if sec_number else None,
                    alternate_names=alternate_names,
                    address=address_element.text.strip() if address_element else None,
                    type=firm_types
                )
            )
        if iadp_only:
            return seq(firms).filter(lambda x: self._check_url(x['url'])).list()
        else:
            return firms

    @staticmethod
    def _check_params(crd, url, base_url):
        if url is None:
            if crd:
                url = base_url.format(crd)
            else:
                raise IAPDError('CRD number or URL required.')
        return url

    def _check_url(self, url):
        if url.startswith(self.BASE_URL):
            return True
        return False

    def get_firm_filings(self, crd=None, url=None, download=False, output_dir=None):
        """
        Get filings of a firm on https://adviserinfo.sec.gov
        Args:
            crd: Central Registration Depository number
            url: link of the firm on the website
            download: download the filings to local storage if set to True
            output_dir: path to directory to store the downloaded filings

        Returns:
            filings url and local path
        """
        url = self._check_params(crd=crd,
                                 url=url,
                                 base_url=self.FIRM_URL)

        response = self._session.get(url)
        soup = BeautifulSoup(response.content, 'lxml')

        adv_one_href, part_2_brochures_href = map(
            lambda x: (soup.find('a', {'id': x}) or {}).get('href', ""), [self.ADV_ONE_HREF_ID, self.ADV_TWO_HREF_ID])

        if part_2_brochures_href.startswith(self.ADV_TWO_BROCHURE_BASE_URL):
            part_2_brochures_href = self._get_adv_two_from_brochures_url(self.BASE_URL + part_2_brochures_href)

        adv_form, part_2_brochures = map(
            lambda href: self.BASE_URL + href if href else "", [adv_one_href, part_2_brochures_href])

        if download:
            adv_form_local_path = self._download_form(url=adv_form,
                                                      output_dir=output_dir)
            part_2_brochures_local_path = self._download_form(url=part_2_brochures,
                                                              output_dir=output_dir)
        else:
            adv_form_local_path = None
            part_2_brochures_local_path = None

        return dict(
            adv_form_url=adv_form,
            adv_form_local_path=adv_form_local_path,
            part_2_brochures_url=part_2_brochures,
            part_2_brochures_local_path=part_2_brochures_local_path
        )

    def get_individual_report(self, crd=None, url=None, download=False, output_dir=None):
        """
        Get a detailed report of an individual on https://adviserinfo.sec.gov
        Args:
            crd: Central Registration Depository number
            url: link of the individual on the website
            download: download the report to local storage if set to True
            output_dir: path directory to store the downloaded report

        Returns:
            detailed report url and local path
        """
        url = self._check_params(crd=crd,
                                 url=url,
                                 base_url=self.INDIVIDUAL_URL)
        response = self._session.get(url)
        soup = BeautifulSoup(response.content, 'lxml')
        detailed_report_url = (soup.find('a', attrs={'id': self.DETAILED_REPORT_ID}) or {}).get('href')
        detailed_report_local_path = self._download_form(url=detailed_report_url,
                                                         output_dir=output_dir) if download else None
        return dict(
            detailed_report_url=detailed_report_url,
            detailed_report_local_path=detailed_report_local_path
        )