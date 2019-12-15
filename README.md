# python-iapd

A script to search and download firm/indivudual data from the Investment Adviser Public Disclosure website (https://adviserinfo.sec.gov/).

Example: 
```
import logging

from iapd.crawler import IAPD
from iapd.utils import crawler_retry

logging.basicConfig(level=logging.DEBUG)


def search_firm(name):
    crawler = IAPD()
    data = []
    for firms in crawler.search(term=name):
        data.extend(firms)
    return data


@crawler_retry(default_value=dict())
def get_filings(crd):
    crawler = IAPD()
    filings = crawler.get_firm_filings(crd=crd,
                                       download=True,
                                       output_dir='path/to/output/dir')   
    return filings
```
