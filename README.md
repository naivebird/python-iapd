# python-iapd

python-iapd is a script created to search and download firm/individual data from the Investment Adviser Public Disclosure website (https://adviserinfo.sec.gov/).

## Installation

```bash
git clone git@github.com:naivebird/python-iapd.git
cd python-iapd
pip install -r requirements.txt
```

## Usage

```python
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

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)
