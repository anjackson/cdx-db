import duckdb
cursor = duckdb.connect()

print(cursor.execute("SELECT * FROM parquet_schema('outdir.parq')").df())

print("--------")
cursor.execute("CREATE VIEW cdx AS SELECT * FROM read_parquet('outdir.parq');")

print("--------")
print(cursor.execute("SELECT * FROM cdx WHERE statuscode > 200 LIMIT 10").df())

print("--------")
print(cursor.execute("SELECT statuscode,count(cdx.offset) FROM cdx GROUP BY statuscode").df())

print("--------")
df = cursor.execute("SELECT mimetype,count(cdx.offset) FROM cdx GROUP BY mimetype").df()
print(df)
