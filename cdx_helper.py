import logging
import requests
from datetime import datetime

logger = logging.getLogger(__name__)

CRAWL_CDX = "http://crawldb-fc.api.wa.bl.uk/fc"
ACCESS_CDX = "http://cdx.api.wa.bl.uk/data-heritrix"
CDX_API = "https://www.webarchive.org.uk/api/mementos/cdx"

class CDX11():    
    def __init__(self, line):
        try:
            self.urlkey, self.timestamp, self.original, self.mimetype, self.statuscode, \
            self.digest, self.redirecturl, self.robotflags, self.length, self.offset, self.filename = line.split(' ')
        except ValueError as e:
            logger.exception(f"Exception when parsing line: {line}",e)
            raise Exception(f"Could not parse line: {line}")

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

    def __init__(self, prefix, cdx_service=CDX_API, scan_limit=100_000_000) -> None:
        self.prefix = prefix
        self.cdx_service = cdx_service
        self.scan_limit = scan_limit
        # Setup accumulators
        self.total_bytes = {}
        self.num_records = {}
        self.num_urls = {}
        self.num_ok_records = {}
        self.num_html_urls = {}
        self.num_dead_urls = {}

    def scan(self):
        # Count records for logging progress:
        record_counter = 0
        # Keep track of the current URL line:
        url_last = None
        # Keep track of the last 200 HTML URL:
        url_ok = None
        # Scan URLs under the prefix:
        for cdx in cdx_scan(self.prefix, cdx_service=self.cdx_service, limit=self.scan_limit):
            year = cdx.timestamp[0:4]
            self.num_records[year] = self.num_records.get(year, 0) + 1
            self.total_bytes[year] = self.total_bytes.get(year, 0) + int(cdx.length)

            # Count unique URLs:
            if url_last is None or cdx.urlkey != url_last.urlkey:
                # Starting a new URL:
                self.num_urls[year] = self.num_urls.get(year, 0) + 1
            url_last = cdx

            # Count all 200s:
            if cdx.statuscode == '200':
                # Count 200 records:
                self.num_ok_records[year] = self.num_ok_records.get(year, 0) + 1

            # Spotting URLs that die is tricky as e.g. https redirects are on the same urlkey, and sometimes the URLs come back.
            # Therefore, we really want the first 200 for each URL, and then the last record that matches it based on the original URL

            # Skip records with unset status code:
            if cdx.statuscode == '0':
                # This seems to be older revisits with no headers, so this is slightly inaccurate:
                continue

            # If there is a url_ok, then we need to track the last exact matching one:
            if url_ok:
                # There's an exact match, or a key match that's not a redirect and so is probably correct (not a redirect):
                if url_ok.original == cdx.original or (url_ok.urlkey == cdx.urlkey and int(int(cdx.statuscode)/100) != 3):
                    url_ok_last = cdx
                # But if we've moved on, we need record the result and clear for a rescan:
                elif url_ok.urlkey != cdx.urlkey:
                    # Record the outcome - what was a 200 is now...
                    if url_ok_last.statuscode != '200': # in ['404', '410', '451']:
                        yield (url_ok_last, url_ok)
                        # Count dead URLs, but ignore redirects as these are usually fine:
                        if int(int(url_ok_last.statuscode)/100) != 3:
                            self.num_dead_urls[year] = self.num_dead_urls.get(year, 0) + 1
                    # Clear the current URL:
                    url_ok = None
                    url_ok_last = None

            # But if we don't have a first URL (url_ok), so we search for one:
            if url_ok == None:
                if cdx.statuscode == '200':
                    if 'html' in cdx.mimetype:
                        # Starting a new URL:
                        self.num_html_urls[year] = self.num_html_urls.get(year, 0) + 1
                        # Update to record the first OK URL
                        url_ok = cdx
                        url_ok_last = cdx
                    
            # Report status occasionally:
            record_counter += 1
            if record_counter%100000 == 0:
                logger.info(f"Processed {record_counter} CDX records...")

