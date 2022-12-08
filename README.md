# cdx-db
Generating Parquet files containing CDX data for SQL queries

Querying an [OutbackCDX](https://github.com/nla/outbackcdx) service, and using [fastparquet](https://fastparquet.readthedocs.io/) to build up a copy of the data in 
[Apache Parquet](https://parquet.apache.org/) files. Then using [DuckDB](https://duckdb.org/docs/data/parquet.html) to query those files using SQL.

The `grab.py` script queries the UKWA API and builds up a Parquet file.  The `query.py` script then runs some SQL queries against the file. e.g. a data frame of status codes in the dataset:

```
cursor.execute("SELECT statuscode,count(*) FROM cdx GROUP BY statuscode ORDER BY count(*) DESC").df()
```

Which looks like:


```
   statuscode  count_star()
0         200        460121
1         404         42536
2         302          1358
3           0          1034
4         301           841
5         400           263
6         304            73
7         403            73
8         502             3
```

## Questions

 - What is the SQL dialect? 
 - How does this compare with plain text and with OutbackCDX index sizes?

## Dead URL Scanner

This repository also contains an experimental dead URL identification procedure.