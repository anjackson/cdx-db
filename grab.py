

# https://www.webarchive.org.uk/api/mementos/cdx?url=http%3A%2F%2Fportico.bl.uk%2F&matchType=prefix&sort=default&limit=10


import requests
import fastparquet
import pandas as pd

KEYS = [
    "urlkey",
    "timestamp",
    "original",
    "mimetype",
    "statuscode",
    "redirect",
    "meta",
    "digest",
    "length",
    "offset",
    "filename"
]


def append(data):
    df = pd.DataFrame(columns=KEYS, data=data)
    print("Writing...")
    fastparquet.write('outdir.parq', df, #row_group_offsets=[0, 10000, 20000],
      compression='GZIP')#, file_scheme='hive')


def stream():
    s = requests.Session()
    r = s.get("https://www.webarchive.org.uk/api/mementos/cdx", 
            params={ 
                'url': 'https://portico.bl.uk/', 
                'matchType': 'prefix',
                #'fl': ','.join(KEYS), Not supported in api yet.
                },
            stream=True)
    data = []
    line_count = 0
    for line in r.iter_lines(decode_unicode=True):
        if line:
            values = line.split(" ")
            for i in [1,4,8,9]:
                if values[i] == '-':
                    values[i] = None
                else:
                    values[i] = int(values[i])
            data.append(values)
            line_count += 1
            if line_count >= 100_000:
                append(data)
                data = []
                line_count = 0
    if line_count > 0:
        append(data)


if __name__ == '__main__':
    stream()
