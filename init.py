from orm.database import SQLDatabase
from orm.table import SQLTable

import orm.db_types
import orm.conn_types

def init_db(dbname, db_type, kwargs):
    import orm.db_types
    db_connector = orm.conn_types.connectors[db_type](**kwargs)
    d = SQLDatabase(dbname, db_type, db_connector, kwargs)
    return init_the_database(d, dbname, db_type, kwargs)

def init_the_database(d, dbname, db_type, kwargs):    
    tables = {}
    
    db_type_definition = getattr(orm.db_types, db_type)
        
    do_get_all_databases = db_type_definition.do_get_all_databases
    do_get_all_tables = db_type_definition.do_get_all_tables
    do_get_all_columns = db_type_definition.do_get_all_columns
    do_get_pkey = db_type_definition.do_get_pkey
    do_get_fkeys = db_type_definition.do_get_fkeys

    c = d.cursor()

    db_data_value = do_get_all_tables(c, dbname)
    column_dict = {}
    pkey_dict = {}
    fkeys_dict = {}

    for i in db_data_value:
        column_dict[i] = do_get_all_columns(c, dbname, i)
        cols, ctypes = column_dict[i]
        pkey_dict[i] = do_get_pkey(c, dbname, i)
        fkeys_dict[i] = do_get_fkeys(c, dbname, i, cols)
    
    for i in db_data_value:
        cols, ctypes = column_dict[i]
        pkey = pkey_dict[i]
        fkeys = fkeys_dict[i]
        t = SQLTable(d, i, pkey, cols, fkeys, ctypes)
        tables[i] = t
        d.tables[i] = t
        
    for i in db_data_value:
        table = tables[i]
        cols, ctypes = column_dict[i]
        pkey = pkey_dict[i]
        fkeys = fkeys_dict[i]
        table.foreign_values = fkeys

    d.calc_refs()
    d.class_updates()
    
    for i in tables:
        setattr(d, i, tables[i])

    return d

