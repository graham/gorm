import orm.table
import orm.query
import orm

from orm.util import SQLNone

class SQLRow(object):
    row_objects = {}
    join_alias = {}
    glue_alias = {}
    
    def __init__(self, database, table, values, joined_table=None, target_table=None):
        self.database = database
        self.table = table
        self.query_index = 0
        self.original_values = {}
        self.last_update_values = {}
        self.updated_values = {}
        self.foreign_data = {}

        self.joined_values = {}
        self.joined_table = joined_table
        self.is_new = False
        self.target_values = {}
        self.fake_values = []
        
        self.values = values

        if values:
            self.is_new = False
            for i in self.values:
                if i in self.table.column_types:
                    self.values[i] = self.table.column_types[i].post_load(self.values[i])
                else:
                    if self.joined_table and i.startswith(self.joined_table.table_name):
                        self.joined_values[i[len(self.joined_table.table_name)+1:]] = values[i]
                        self.fake_values.append(i)                        
                    elif target_table and i.startswith(target_table.table_name):
                        self.target_values[i[len(target_table.table_name)+1:]] = values[i]
                        self.fake_values.append(i)
                    else:
                        self.joined_values[i] = values[i]

            self.original_values = self.values.copy()
        else:
            self.is_new = True
            for i in self.table.columns:
                self.values[i] = SQLNone()

        for i in self.target_values:
            self.values[i] = self.target_values[i]
        
        for i in self.fake_values:
            self.values.pop(i)
                
        for i in self.table.columns:
            self.__dict__[i] = 'PLACEHOLDER'
        
        for i in self.join_alias:
            self.__dict__[i] = 'PLACEHOLDER'
        
        for i in self.glue_alias:
            self.__dict__[i] = 'PLACEHOLDER'
            
        self.clean_values()
            
    def clean_values(self):
        for i in self.values:
            val = self.values[i]
            if type(self.table.column_types[i]) == orm.sql_types.SQLBlob:
                pass
            elif type(val) == str or type(val) == unicode:
                self.values[i] = unicode(val, errors='replace')

    def __cmp__(self, obj):
        if obj == None:
            return True
        if type(obj) == int:
            return int(self.get_primary()) != obj
        elif issubclass(obj.__class__, SQLRow):
            if self.get_primary() == None or obj.get_primary() == None:
                return True
            
            if obj.table == self.table:
                return int(self.get_primary()) != int(obj.get_primary())
            else:
                return True
        else:
            return True
        
    def __getattribute__(self, key):
        tmp = object.__getattribute__(self, key)
        if key != 'values' and key != 'join_alias' and key != 'glue_alias':
            if getattr(self, 'values', None):
                if self.values.has_key(key):
                    if key in self.table.foreign_values.keys():
                        val = None
                        if self.foreign_data.has_key(key):
                            val = self.foreign_data[key]
                        else:
                            t = self.database.get_table(self.table.foreign_values[key])
                            if self.values[key] == None:
                                return None
                            val = t.get(self.values[key])
                            self.foreign_data[key] = val
                        return val
                    else:
                        val = self.__dict__['values'][key]
                        if self.table.column_types[key].dirty_on_touch:
                            self.updated_values[key] = True
                        return val
            if getattr(self, 'join_alias', None):
                if key in self.join_alias:
                    return self.join(self.join_alias[key])
            if getattr(self, 'glue_alias', None):
                if key in self.glue_alias:
                    return self.glue(self.glue_alias[key])
        return tmp
        
                    
    def __setattr__(self, key, value):
        if 'values' in self.__dict__:
            if getattr(value, 'get_primary', False):
                value = value.get_primary()
            if self.__dict__['values'].has_key(key):
                if self.__dict__['values'][key] == value:
                    pass
                else:
                    self.updated_values[key] = True 
                self.__dict__['values'][key] = value
                if self.foreign_data.has_key(key):
                    del self.foreign_data[key]
            else:
                self.__dict__[key] = value
        else:
            return object.__setattr__(self, key, value)


    def __repr__(self):
        r = "<Row from %s" % self.table.table_name
        r += " (%s)>" % ', '.join( ["%s=%r" % (i, self.values[i]) for i in self.values] )
        return r

    def reload(self):
        self.values = self.table.get( self.get_primary() ).values

    def get_primary(self):
        if self.table.primary_key:
            if self.table.primary_key in self.values:
                return self.values[self.table.primary_key]
        return None

    def safe_update(self, **kwargs):
        if self.table.primary_key in kwargs:
            kwargs.pop(self.table.primary_key)
        
        exclude = []
        if '__exclude__' in kwargs:
            exclude = kwargs['__exclude__']
            kwargs.pop('__exclude__')
        
        for i in exclude:
            kwargs.pop(i)
            
        self.update(**kwargs)

    def update(self, **kwargs):
        for i in kwargs.keys():
            if i in self.table.columns:
                if getattr(kwargs[i], 'get_primary', False):
                    self.values[i] = kwargs[i].get_primary()
                    self.foreign_data[i] = kwargs[i]
                else:
                    self.values[i] = kwargs[i]
                self.updated_values[i] = True
            else:
                print 'Missing: %s' % i
    

    def delete(self, cascade=0):
        self.database.db_connector.delete_row(self, cascade)
        
    def get_linked(self):        
        rows = []
        for i in self.table.refer_to_me:
            t = self.database.get_table(i[0])
            for j in t.find( getattr(t.q, i[1]).equals(self.get_primary()) ):
                rows.append(j)        
        return rows
        
    def unlink(self):
        rows = []
        
        for i in self.table.refer_to_me:
            t = self.database.get_table(i[0])
            #t.delete( getattr(t.q, i[1]).equals(self.get_primary()) )
            for j in t.find( getattr(t.q, i[1]).equals(self.get_primary()) ):
                rows.append(j)
                setattr(j, i[1], None)
                j.save()
        
        return rows
        
    def relink(self, id):
        rows = []
        
        for i in self.table.refer_to_me:
            t = self.database.get_table(i[0])
            #t.delete( getattr(t.q, i[1]).equals(self.get_primary()) )
            for j in t.find( getattr(t.q, i[1]).equals(self.get_primary()) ):
                rows.append(j)
                setattr(j, i[1], id)
                j.save()
        
        return rows
        
    def save(self, cursor=None, silent=1, reflect=1):
        if cursor == None:
            cursor = self.database.cursor()
            
        if self.is_new:            
            self.database.db_connector.save_row(self, cursor, silent, reflect)
        else:
            self.database.db_connector.update_row(self, cursor, silent, reflect)
            
        for i in self.updated_values:
            if self.updated_values[i]:
                self.last_update_values[i] = self.values[i]
        self.updated_values = {}
        self.is_new = 0
            
        for i in self.values:
            if type(self.values[i]) == SQLNone:
                self.values[i] = None

        return self

    def join(self, table_name, optional_column_name=None, limit=0, offset=0, only=None, exclude=None, order=''):
        if table_name.__class__ == orm.table.SQLTable:
            table_name = table_name.table_name

        found = False

        for i in self.table.refer_to_me:
            if optional_column_name == None and i[0] == table_name:
                t = self.database.get_table(table_name)
                ret = t.match(**{i[1]:self.get_primary(), 'limit':limit, 'offset':offset, 'order':order, 'only':only, 'exclude':exclude}) 
                #ret = t.find(where='"%s"='%i[1]+'%s', vars=(self.get_primary(), ), limit=limit, order=order)
                return ret
            elif optional_column_name != None:
                if optional_column_name == i[1] and i[0] == table_name:
                    t = self.database.get_table(table_name)
                    ret = t.match(**{i[1]:self.get_primary(), 'limit':limit, 'offset':offset, 'order':order, 'only':only, 'exclude':exclude}) 
                    #ret = t.find(where='"%s"='%i[1]+'%s', vars=(self.get_primary(), ), limit=limit, order=order)
                    return ret

        raise Exception("Table %s does not refer to this one." % table_name)

    def get_glue_table(self, table_name, optional_column_name=None):
        if table_name.__class__ == orm.table.SQLTable:
            table_name = table_name.table_name

        if self.database.get_table(table_name):
            for i in self.table.refer_to_me:
                if i[0] == table_name and optional_column_name == None:
                    join_table = self.database.get_table(table_name)

                    if len(join_table.foreign_values.keys()) > 2:
                        raise Exception("You must specify the joiner")
                               
                    keys = join_table.foreign_values.keys()
                    target_table = None
                    ref_key = None
                    for_key = None

                    if join_table.foreign_values[keys[0]] != self.table.table_name:
                        target_table = self.database.get_table(join_table.foreign_values[keys[0]])
                        ref_key = keys[1]
                        for_key = keys[0]
                    elif join_table.foreign_values[keys[1]] != self.table.table_name:
                        target_table = self.database.get_table(join_table.foreign_values[keys[1]])
                        ref_key = keys[0]
                        for_key = keys[1]
                        
                    return target_table
                elif i[0] == table_name and optional_column_name != None:
                    join_table = self.database.get_table(table_name)

                    keys = join_table.foreign_values.keys()
                    target_table = None
                    ref_key = None
                    for_key = None

                    target_table = self.database.get_table(join_table.foreign_values[optional_column_name])
                    ref_key = self.table.table_name
                    for_key = optional_column_name
                    
                    return target_table
                            
    def glue(self, table_name, optional_column_name=None, ref_me=None, limit=None, offset=None, only=None, exclude=None, order=None):
        if table_name.__class__ == orm.table.SQLTable:
            table_name = table_name.table_name

        if self.database.get_table(table_name):
            for i in self.table.refer_to_me:
                if i[0] == table_name and optional_column_name==None:
                    join_table = self.database.get_table(table_name)

                    if len(join_table.foreign_values.keys()) > 2:
                        raise Exception("You must specify the joiner")
                               
                    keys = join_table.foreign_values.keys()
                    target_table = None
                    ref_key = None
                    for_key = None

                    if join_table.foreign_values[keys[0]] != self.table.table_name:
                        target_table = self.database.get_table(join_table.foreign_values[keys[0]])
                        ref_key = keys[1]
                        for_key = keys[0]
                    elif join_table.foreign_values[keys[1]] != self.table.table_name:
                        target_table = self.database.get_table(join_table.foreign_values[keys[1]])
                        ref_key = keys[0]
                        for_key = keys[1]
                    else:
                        print 'PROBLEM'
                    
                    first_columns = ', '.join( [ '"%s"."%s" as "%s.%s"' % (join_table.table_name, i, join_table.table_name, i) for i in join_table.columns] )
                   
                    target_columns = [iii for iii in target_table.columns]
                    
                    if exclude:
                        for k in exclude:
                            if k in target_columns:
                                target_columns.remove(k)
                                
                    if only:
                        for k in only:
                            if k not in only:
                                target_columns.remove(k)
                    
                    target_column_query = ', '.join([ '"%s"."%s" as "%s.%s"' % (target_table.table_name, i, target_table.table_name, i) for i in target_columns] )
                    
                    q = 'SELECT %s, %s from \"%s\", \"%s\" WHERE \"%s\".\"%s\"=%s and \"%s\".\"%s\"=\"%s\".\"%s\"'
                    vals = (first_columns, target_column_query, target_table.table_name, join_table.table_name, join_table.table_name,  ref_key, self.database.db_connector.escape_character(), join_table.table_name, for_key, target_table.table_name, target_table.primary_key)

                    if limit:
                        q += ' LIMIT %i ' % limit
                    if offset:
                        q += ' OFFSET %i ' % offset
                    if order:
                        q += ' ORDER BY %s ' % order

                    ret = target_table.glue_result_from_query(join_table, target_table, q % vals, (self.get_primary(), ))
                    return ret
                    
                elif i[0] == table_name and optional_column_name != None:
                    join_table = self.database.get_table(table_name)

                    keys = join_table.foreign_values.keys()
                    target_table = None
                    ref_key = None
                    for_key = None

                    target_table = self.database.get_table(join_table.foreign_values[optional_column_name])
                    ref_key = self.table.table_name
                    for_key = optional_column_name
                    
                    if ref_me == None:
                        for ref_table_name, ref_name in self.table.refer_to_me:
                            if ref_table_name == table_name:
                                ref_me = ref_name
                                break
                                
                    first_columns = ', '.join( [ '"%s"."%s" as "%s.%s"' % (join_table.table_name, i, join_table.table_name, i) for i in join_table.columns] )
                   
                    target_columns = [iii for iii in target_table.columns]
                    
                    if exclude:
                        for k in exclude:
                            if k in target_columns:
                                target_columns.remove(k)
                                
                    if only:
                        for k in only:
                            if k not in only:
                                target_columns.remove(k)
                    
                    target_column_query = ', '.join([ '"%s"."%s" as "%s.%s"' % (target_table.table_name, i, target_table.table_name, i) for i in target_columns] )
                    
                    q = 'SELECT %s, %s from \"%s\", \"%s\" WHERE \"%s\".\"%s\"=%s and \"%s\".\"%s\"=\"%s\".\"%s\"'
                    vals = (first_columns, target_column_query, target_table.table_name, join_table.table_name, join_table.table_name, ref_me, self.database.db_connector.escape_character(), join_table.table_name, for_key, target_table.table_name, target_table.primary_key)

                    if limit:
                        q += ' LIMIT %i ' % limit
                    if offset:
                        q += ' OFFSET %i ' % offset
                    if order:
                        q += ' ORDER BY %s ' % order

                    ret = target_table.glue_result_from_query(join_table, target_table, q % vals, (self.get_primary(), ))
                    return ret                    

                                            
            raise Exception("For some reason i dont like %s." % table_name)
        else:
            raise Exception("Table %s does not exist." % table_name)
            
    def clean_last_update(self):
        self.last_update_values = {}
        
    def __json__(self):
        return self.values
        
    def as_sql(self, with_id=1):
        fields = []
        for i in self.table.columns:
            if not with_id and i == self.table.primary_key:
                continue
            
            fields.append('"'+i+'"')
        
        values = []
        for i in self.table.columns:
            if not with_id and i == self.table.primary_key:
                continue
                
            if type(self.values[i]) == str:
                values.append("'%s'" % self.values[i])
            elif self.values[i] == None:
                values.append("NULL")
            else:
                values.append("%s" % self.values[i])
                
        return 'INSERT INTO "%s" (%s) values(%s);' % (self.table.table_name, ', '.join(fields), ','.join(values))
        
    
    def duplicate(self):
        tmp = self.table.create()
        for i in self.values:
            if i == self.table.primary_key:
                pass
            else:
                setattr(tmp, i, self.values[i])
        return tmp
        
    def __del__(self):
        # if we are in some sort of debug mode lets make sure that
        # the person coding remembers to save it.
        if self.is_new or len(self.updated_values):
            for i in self.updated_values:
                if self.table.column_types[i].dirty_on_touch:
                    pass
                else:
                    print '*' * 3, self.table.table_name, 'HEY IDIOT CALL .save()', "You changed %s and didnt save. ***" % i    