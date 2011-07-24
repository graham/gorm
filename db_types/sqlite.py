from orm.sql_types import *

import re

def do_get_all_databases(cursor):
    return ['sqlite3']

def do_get_all_tables(cursor, dbname=None):
    cursor.execute("select tbl_name from sqlite_master where type='table'", log_query=0)
    return [str(i['tbl_name']) for i in cursor.fetchall()]

def do_get_all_columns(cursor, dbname, table_name):
    cursor.execute("select sql from sqlite_master where tbl_name='%s'" % table_name, log_query=0)
    schema = str(cursor.fetchone()['sql'].strip())
    start = schema.find('(')
    end = schema.rfind(')')
    schema = schema[start+1:end].strip()

    cols = []
    r = {}

    for i in schema.split(','):
        i = i.strip()
        j = i.split()
        rest = i[2:]

        if len(j) < 2:
            continue

        if j[0].startswith('"') and j[0].endswith('"'):
            j[0] = j[0][1:-1]
        
        if j[1] in types.keys():
            cols.append(j[0])
            r[j[0]] = types[j[1]]()
        elif 'varchar' in j[1]:
            cols.append(j[0])
            r[j[0]] = SQLVarChar()
        elif j[0] in constraints:
            #print "Skipping %s in %s cause it is a constraint" % (j[0], table_name)
            pass
        else:
            print dbname, table_name, 'cant find type for %s' % str(j)
        
    return cols, r

def do_get_pkey(cursor, dbname, table_name):
    sql = cursor.execute("select sql from sqlite_master where tbl_name=?", (table_name, ), log_query=0)
    schema = str(cursor.fetchone()['sql'].strip())
    start = schema.find('(')
    end = schema.rfind(')')
    schema = schema[start+1:end].strip()

    for i in schema.split(','):
        i = i.strip()
        j = i.split()
        
        if j[0].startswith('"') and j[0].endswith('"'):
            j[0] = j[0][1:-1]

        rest = ' '.join(j[2:])
        rest = rest.lower()
        if 'primary key' in rest:
            return j[0]
            
    return None
        
def do_get_fkeys(cursor, dbname, table_name, columns):
    sql = cursor.execute("select sql from sqlite_master where tbl_name=?", (table_name, ), log_query=0)
    schema = str(cursor.fetchone()['sql'].strip())
    start = schema.find('(')
    end = schema.rfind(')')
    schema = schema[start+1:end].strip()

    fkeys = {}

    for i in schema.split(','):
        i = i.strip()
        j = i.split()

        if j[0].startswith('"') and j[0].endswith('"'):
            j[0] = j[0][1:-1]

        rest = ' '.join(j[2:])
        if 'references' in rest:
            m = re.match('references (.*)\((.*)\)', rest)
            table, col = m.groups()
            fkeys[j[0]] = str(table)
    
    return fkeys

    
