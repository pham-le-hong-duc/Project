import duckdb
import os
''' set up and create datawarehouse'''

# connect to duckdb
duck_path='/mnt/d/learn/DE/Semina_project/datawarehouse.duckdb'

if os.path.exists(duck_path):
    os.remove(duck_path)

con=duckdb.connect(duck_path)

# scrip create table
with open('/mnt/d/learn/DE/Semina_project/SQL_db/config_database/source_db.sql', 'r') as f:
    sql_script = f.read()
con.execute(sql_script)

con.close()
