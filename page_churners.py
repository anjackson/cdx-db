import logging
from cdx_helper import cdx_scan, CDX_API

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Okay, needs to be a bit cleverer and not pick up on transient 404s (or possibly class them differently).
# i.e. if you hit a 200, that resets the 404. The first 404 sets up the pair, 
# but the pair is only reported if we hit the end of the set of results for that URL.

bbc = 'https://www.bbc.co.uk/news/'
mirror = 'https://www.mirror.co.uk/'
express = 'https://www.express.co.uk/'
guardian = 'https://www.theguardian.com/'
govuk = 'https://www.gov.uk/'
portico = 'http://portico.bl.uk/'

#prefixes = [bbc, mirror, express, guardian]
prefixes = [portico]

class DeadURLScanner():
    # Setup accumulators
    total_bytes = 0
    num_records = 0
    num_urls = 0
    num_html_records = 0
    num_html_urls = 0
    num_dead_urls = 0

    def __init__(self, prefix) -> None:
        self.prefix = prefix

    def scan(self):
        # Keep track of the current URL line:
        url_last = None
        # Keep track of the last 200 HTML URL:
        url_ok = None
        # Scan URLs under the prefix:
        for cdx in cdx_scan(prefix, cdx_service=CDX_API, limit=100_000_000):
            self.num_records += 1
            self.total_bytes += int(cdx.length)

            # Count unique records
            if url_last is None or cdx.original != url_last.original:
                # Starting a new URL:
                self.num_urls += 1
            url_last = cdx

            if cdx.statuscode == '200':
                if 'html' in cdx.mimetype:
                    if url_ok is None or cdx.original != url_ok.original:
                        # Starting a new URL:
                        self.num_html_urls += 1
                        # Update to record the first OK URL
                        url_ok = cdx
                    # Count 200 HTML records:
                    self.num_html_records += 1
            #elif int(int(cdx.statuscode)/100) == 4: # mostly interested in 404 but considering 4xx - too many 403?!?
            elif cdx.statuscode in ['404', '410', '451']:
                if url_ok and cdx.original == url_ok.original:
                    yield (cdx, url_ok)
                    # Drop the OK so we don't add the same URL many times:
                    url_ok = None
                    # Count dead URLs
                    self.num_dead_urls += 1
            # Report status occasionally:
            if self.num_records%100000 == 0:
                logger.info(f"Processed {self.num_records} CDX records...")


for prefix in prefixes:
    scanner = DeadURLScanner(prefix)
    for ded, ok in scanner.scan():
        print(ded.original, ok.timestamp, ok.statuscode, ded.timestamp, ded.statuscode)

    print(scanner.num_records, scanner.num_urls, scanner.num_html_records, scanner.num_html_urls, scanner.num_dead_urls)


