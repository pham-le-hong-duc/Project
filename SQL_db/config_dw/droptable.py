import duckdb
path='/mnt/d/learn/DE/Semina_project/datawarehouse.duckdb'
con = duckdb.connect(path)
list_tables=con.execute('PRAGMA show_tables').fetchall()
table_names = [row[0] for row in list_tables]
for table_name in table_names:
    print(con.sql(f'drop table if exists {table_name}'))
con.close()