import json
import logging
import argparse
from cdx_helper import DeadURLScanner, CDX_API

logger = logging.getLogger(__name__)

# Okay, needs to be a bit cleverer and not pick up on transient 404s (or possibly class them differently).
# i.e. if you hit a 200, that resets the 404. The first 404 sets up the pair, 
# but the pair is only reported if we hit the end of the set of results for that URL.

def scan_prefix(prefix):
    scanner = DeadURLScanner(prefix)
    for ded, ok in scanner.scan():
        hit = {
            'class': 'dead-url',
            'prefix': prefix,
            'url': ded.original,
            'first_ts': ok.timestamp,
            'first_status': ok.statuscode,
            'dead_ts': ded.timestamp,
            'dead_status': ded.statuscode
        }
        print(json.dumps(hit))
    stats = {
        'class': 'prefix-stats',
        'prefix': prefix,
        'num_records': scanner.num_records, 
        'num_urls': scanner.num_urls, 
        'num_html_records': scanner.num_html_records, 
        'num_html_urls': scanner.num_html_urls, 
        'num_dead_urls': scanner.num_dead_urls
    }
    print(json.dumps(stats))

def main():
    # Set up a parser:
    parser = argparse.ArgumentParser(prog='churners',formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-v', '--verbose',  action='count', default=0, help='Logging level; add more -v for more logging.')
    parser.add_argument('-C', '--cdx-service', default=CDX_API, help="The CDX Service to talk to.")
    parser.add_argument('prefixes', help="A file containing a list of URL prefixes to scan")

    # And PARSE it:
    args = parser.parse_args()

    # Set up verbose logging:
    if args.verbose == 1:
        logging.getLogger().setLevel(logging.INFO)
    elif args.verbose >= 2:
        logging.getLogger().setLevel(logging.DEBUG)

    with open(args.prefixes) as f:
        for line in f:
            line = line.strip()
            if line:
                scan_prefix(line)

if __name__ == "__main__":
    main()