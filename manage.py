import orm
import orm.init
import orm.condition

def create_preset(name, db_type, **kwargs):
    print 'creating database preset for -> %s' % name
    orm.database_presets[name] = (name, db_type, kwargs)
    if orm.default_database:
        pass
    else:
        orm.q = orm.condition.AnonymousBuilder(orm.load_preset(name), db_type)
    
def remove_preset(name):
    orm.database_presets[name].pop()

def get_presets():
    return orm.database_presets.keys()

def set_default_preset(name):
    import orm.table
    for i in dir(orm):
        if type(getattr(orm, i)) == orm.table.SQLTable:
            del(orm.i)

    preset = load_preset(name)
    orm.default_database = preset
    orm.q = orm.condition.AnonymousBuilder(preset, orm.default_database.db_type)

def does_preset_exist(name):
    if not name:
        return False
        
    if name in orm.database_presets:
        return True
    else:
        return False

def load_preset(name):
    if name not in orm.database_presets:
        raise Exception("Preset %s not found." % name)
        
    if name not in orm.database_objects:
        orm.database_objects[name] = orm.init.init_db(*orm.database_presets[name])
    return orm.database_objects[name]

def create_and_load_preset(name, db_type, **kwargs):
    create_preset(name, db_type, **kwargs)
    return load_preset(name)
    
def create_or_load_preset(name, db_type, **kwargs):
    if does_preset_exist(name):
        return load_preset(name)
    else:
        create_preset(name, db_type, **kwargs)
        return load_preset(name)

def quick_load(db_type, **kwargs):
    return orm.init.init_db('None', db_type, kwargs)

def temp_db(name):
    import tempfile

    tempfile.tempdir = '/tmp/'
    filename = tempfile.mktemp(prefix='orm_db_test_')
    return quick_load('sqlite', filename=filename)

def memory_db(name):
    return quick_load('sqlite', filename=':memory:')
    