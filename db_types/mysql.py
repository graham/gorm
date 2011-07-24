from orm.sql_types import *

get_all_databases = 'select schema_name from information_schema.schemata'

get_all_tables = "select table_name from information_schema.tables where table_schema='%s'"

get_all_columns = "SELECT column_name, data_type from information_schema.columns WHERE table_schema='%s' and table_name='%s' order by ordinal_position;"

get_all_pkeys = "select column_name, constraint_name, column_name from information_schema.key_column_usage where constraint_schema='%s' and constraint_name='PRIMARY' and table_name='%s';"

def do_get_all_databases(cursor):
    cursor.execute(get_all_databases)
    return [i['datname'] for i in cursor.fetchall()]

def do_get_all_tables(cursor, dbname):
    cursor.execute(get_all_tables % dbname)
    tables = [i['table_name'] for i in cursor.fetchall()]
    return tables

def do_get_all_columns(cursor, dbname, table_name):
    cursor.execute(get_all_columns % (dbname, table_name))
    results = cursor.fetchall()
    cols = []
    r = {}
    for i in results:
        cols.append(i['column_name'])
        if i['data_type'] in types.keys():
            r[i['column_name']] = types[i['data_type']]()
        else:
            r[i['column_name']] = SQLUnknown(table_name, i['column_name'], i['data_type'])
        
    return cols, r

def do_get_pkey(cursor, dbname, table_name):
    cursor.execute(get_all_pkeys % (dbname, table_name))
    result = cursor.fetchone()
    if result:
        return result['column_name']
    else:
        return None

def do_get_fkeys(cursor, dbname, table_name, columns):
    return {}
