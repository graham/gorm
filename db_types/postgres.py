from orm.sql_types import *
get_all_databases = 'select datname from pg_database;'

get_all_tables = "select tablename from pg_catalog.pg_tables where schemaname='public' order by tablename;"
get_all_columns = "SELECT column_name, data_type from information_schema.columns WHERE table_name='%s' order by ordinal_position;"
get_all_pkeys = "select column_name, constraint_name, table_name from information_schema.constraint_column_usage where table_name='%s' and (constraint_name like '%%_pkey');"
get_all_fkeys2 = '''
select t.tgconstrname, c1.relname as tablename, c2.relname as foreigntable
   FROM pg_trigger t
         left join pg_class c1 on t.tgrelid = c1.relfilenode
         left join pg_class c2 on t.tgconstrrelid = c2.relfilenode
   where c1.relname='%s' and substr(t.tgname, 1, 3) = 'RI_' order by t.tgconstrname;
'''
get_all_fkeys4 = "select c.conname, cl.relname from pg_constraint c, pg_class cl where cl.oid=c.conrelid and c.contype='f' and cl.relname='%s';"

def do_get_all_databases(cursor):
    cursor.execute(get_all_databases, log_query=0)
    return [i['datname'] for i in cursor.fetchall()]

def do_get_all_tables(cursor, dbname=None):
    cursor.execute(get_all_tables, log_query=0)
    return [i['tablename'] for i in cursor.fetchall()]

def do_get_all_columns(cursor, dbname, table_name):
    cursor.execute(get_all_columns % table_name, log_query=0)
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
    cursor.execute(get_all_pkeys % table_name, log_query=0)
    result = cursor.fetchone()
    if result:
        return result['column_name']
    else:
        return None

def do_get_fkeys(cursor, dbname, table_name, columns):
    cursor.execute(get_all_fkeys4 % table_name, log_query=0)
    result = cursor.fetchall()
    fkeys = {}

    for i in result:
        cursor.execute(get_all_fkeys2 % table_name, log_query=0)
        for j in cursor.fetchall():
            table = j['tablename']
            for_table = j['foreigntable']
            column = j['tgconstrname']

            if for_table == None:
                continue

            newcolumn = ''

            if column.startswith(table):
                newcolumn = column[len(table)+1:].rsplit("_", 1)[0]
            else:
                newcolumn = column.rsplit("_", 3)[0]

            if newcolumn in columns:
                fkeys[newcolumn] = for_table

    return fkeys
    
