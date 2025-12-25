import duckdb
path='/mnt/d/learn/DE/Semina_project/datawarehouse.duckdb'
con = duckdb.connect(path)
list_tables=con.execute('PRAGMA show_tables').fetchall()
table_names = [row[0] for row in list_tables]
print(con.sql('pragma show_tables;'),'\n')
for table_name in table_names:
    print(con.sql(f'select count(1) as {table_name} from {table_name}'))
con.close()