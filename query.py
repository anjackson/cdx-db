import duckdb
cursor = duckdb.connect()

parq_file = 'output.parq'

print(cursor.execute(f"SELECT * FROM parquet_schema('{parq_file}')").df())

print("--------")
cursor.execute(f"CREATE VIEW cdx AS SELECT * FROM read_parquet('{parq_file}');")

print("--------")
print(cursor.execute("SELECT original,statuscode,count(*) FROM cdx WHERE statuscode > 200 GROUP BY original,statuscode ORDER BY count(*) DESC LIMIT 10").df())

print("--------")
print(cursor.execute("SELECT statuscode,count(*) FROM cdx GROUP BY statuscode ORDER BY count(*) DESC").df())

print("--------")
df = cursor.execute("SELECT mimetype,count(*) FROM cdx GROUP BY mimetype ORDER BY count(*) DESC").df()
print(df)
