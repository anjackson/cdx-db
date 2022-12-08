import logging
import requests
from datetime import datetime

logger = logging.getLogger(__name__)

CRAWL_CDX = "http://crawldb-fc.api.wa.bl.uk/fc"
ACCESS_CDX = "http://cdx.api.wa.bl.uk/data-heritrix"
CDX_API = "https://www.webarchive.org.uk/api/mementos/cdx"

class CDX11():    
    def __init__(self, line):
        self.urlkey, self.timestamp, self.original, self.mimetype, self.statuscode, \
        self.digest, self.redirecturl, self.robotflags, self.length, self.offset, self.filename = line.split(' ')

    def __str__(self):
        return ' '.join([self.urlkey, self.timestamp, self.original, self.mimetype, self.statuscode, \
        self.digest, self.redirecturl, self.robotflags, self.length, self.offset, self.filename])
    
    @property
    def crawl_date(self):
        return datetime.strptime(self.timestamp, '%Y%m%d%H%M%S')
    
    def to_dict(self):
        return {
            'urlkey': self.urlkey,
            'timestamp': self.timestamp, 
            'crawl_date': self.crawl_date,
            'original': self.original,
            'mimetype': self.mimetype,
            'statuscode': self.statuscode,
            'digest': self.digest, 
            'redirecturl': self.redirecturl, 
            'robotflags': self.robotflags, 
            'length': int(self.length), 
            'offset': int(self.offset), 
            'filename': self.filename
        }

def cdx_query(url, cdx_service=ACCESS_CDX, limit=25, sort='reverse', from_ts=None, to_ts=None):
    p = { 'url' : url, 'limit': limit, 'sort': sort }
    if from_ts:
        p['from'] = from_ts
    r = requests.get(cdx_service, params = p, stream=True )
    if r.status_code == 200:
        for line in r.iter_lines(decode_unicode=True):
            cdx = CDX11(line)
            yield cdx
    elif r.status_code != 404:
        print("ERROR! %s" % r)

def cdx_scan(url, cdx_service=ACCESS_CDX, limit=10000):
    p = { 'url' : url, 'limit': limit, 'matchType': 'prefix' }
    # Call:
    r = requests.get(cdx_service, params = p, stream=True )
    if r.status_code == 200:
        for line in r.iter_lines(decode_unicode=True):
            cdx = CDX11(line)
            yield cdx
    elif r.status_code != 404:
        print("ERROR! %s" % r)
        print(r.text)


class DeadURLScanner():
    # Setup accumulators
    total_bytes = {}
    num_records = {}
    num_urls = {}
    num_html_records = {}
    num_html_urls = {}
    num_dead_urls = {}

    def __init__(self, prefix, cdx_service=CDX_API) -> None:
        self.prefix = prefix
        self.cdx_service = cdx_service

    def scan(self):
        # Count records for logging progress:
        record_counter = 0
        # Keep track of the current URL line:
        url_last = None
        # Keep track of the last 200 HTML URL:
        url_ok = None
        # Scan URLs under the prefix:
        for cdx in cdx_scan(self.prefix, cdx_service=self.cdx_service, limit=100_000_000):
            year = cdx.timestamp[0:4]
            self.num_records[year] = self.num_html_records.get(year, 0) + 1
            self.total_bytes[year] = self.total_bytes.get(year, 0) + int(cdx.length)

            # Count unique records
            if url_last is None or cdx.original != url_last.original:
                # Starting a new URL:
                self.num_urls[year] = self.num_urls.get(year, 0) + 1
            url_last = cdx

            if cdx.statuscode == '200':
                if 'html' in cdx.mimetype:
                    if url_ok is None or cdx.original != url_ok.original:
                        # Starting a new URL:
                        self.num_html_urls[year] = self.num_html_urls.get(year, 0) + 1
                        # Update to record the first OK URL
                        url_ok = cdx
                    # Count 200 HTML records:
                    self.num_html_records[year] = self.num_html_records.get(year, 0) + 1
            #elif int(int(cdx.statuscode)/100) == 4: # mostly interested in 404 but considering 4xx - too many 403?!?
            elif cdx.statuscode in ['404', '410', '451']:
                if url_ok and cdx.original == url_ok.original:
                    yield (cdx, url_ok)
                    # Drop the OK so we don't add the same URL many times:
                    url_ok = None
                    # Count dead URLs
                    self.num_dead_urls[year] = self.num_dead_urls.get(year, 0) + 1
            # Report status occasionally:
            record_counter += 1
            if record_counter%100000 == 0:
                logger.info(f"Processed {record_counter} CDX records...")

