import os
import orm

class Mock(object):
    pass
    
orm_mock = Mock()
orm_mock.level = None

def verbose_level():
    if orm_mock.level == None:
        import sys
        for i in sys.argv:
            if '-v' in i:
                orm_mock.level = i.count('v')
            
        if orm_mock.level == None:
            orm_mock.level = 0
    return orm_mock.level

if verbose_level():
    print "orm loading: verbose level %i." % verbose_level()

database_presets = {}
database_objects = {}
default_database = None

def get_table(name):
    if default_database:
        return default_database.get_table(name)
    else:
        return None

def refresh():
    for i in database_objects:
        database_objects[i].class_updates()

from orm.manage import *
from orm.table import *
from orm.row import *
from orm.init import *
# 
# import orm
# import jitsu_config.db_config
# x = orm.load_preset('jitsu_db')

try:
    from db_config import *
    import db_config
    if verbose_level():
        print "Loading config from", os.path.abspath(db_config.__file__)
except ImportError, ie:
    if verbose_level():
        print 'Couldnt find a db_config.py to import so running with defaults'
except:
    import traceback
    traceback.print_exc()    
    if verbose_level():
        print 'Couldnt find a db_config.py to import so running with defaults'

def query_count():
    return orm.cursor.query_count[0]

from orm.util import *