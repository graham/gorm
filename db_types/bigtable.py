# This file should be able to load a flat file for use.
# I figure this will be most often used to determine if a database is
# the same as the schema doc provided.

from orm.sql_types import *

import re

def do_get_all_databases(cursor):
    return ['bigtable']

def do_get_all_tables(cursor, dbname=None):
    from google.appengine.ext import db
    return [i.__name__ for i in orm.util.get_all_subclasses(db.Model)]

def do_get_all_columns(cursor, dbname, table_name):
    from google.appengine.ext import db    
    table = None
    for i in orm.util.get_all_subclasses(db.Model):
        if i.__name__ == table_name:
            table = i
    
    colnames = table._properties.keys()
    return colnames, {}
    
def do_get_pkey(cursor, dbname, table_name):
    return None
        
def do_get_fkeys(cursor, dbname, table_name, columns):
    return {}
    
