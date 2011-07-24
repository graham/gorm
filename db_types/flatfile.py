# This file should be able to load a flat file for use.
# I figure this will be most often used to determine if a database is
# the same as the schema doc provided.

from orm.sql_types import *

import re

def do_get_all_databases(cursor):
    return ['flatfile']

def do_get_all_tables(cursor, dbname=None):
    return []
    
def do_get_all_columns(cursor, dbname, table_name):
    return [], {}
    
def do_get_pkey(cursor, dbname, table_name):
    return None
        
def do_get_fkeys(cursor, dbname, table_name, columns):
    return {}
    
