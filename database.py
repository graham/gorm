import orm
import orm.init
from orm.cursor import Cursor
import threading
import traceback

class SQLDatabase(object):
    def __init__(self, name, db_type, db_connector, init_kwargs):
        self.connection_pool = {}
        self.db_connector = db_connector
        self.name = name
        self.init_kwargs = init_kwargs

        self.last_connection = None
        self.in_memory = False

        if 'filename' in init_kwargs:
            if init_kwargs['filename'] == ':memory:':
                self.in_memory = True
                
        self.db_type = db_type
        self.db_version = '3'
        
        self.tables = {}
        self.foreign_tables = {}
        self.foreign_databases = []
        
    def __getattribute__(self, key):
        if key not in ('tables', 'get_table', 'foreign_tables', 'foreign_databases'):
            tmp = self.get_table(key)
            if tmp:
                return tmp
                
        tmp = object.__getattribute__(self, key)
        return tmp
    
    def create_table(self, table_name, buf, cursor=None):
        if not cursor:
            cursor = self.cursor()
        cursor.execute(("create table %s (" % table_name) +buf+");", [])
        return cursor.fetchall()
        
    def reload(self):
        self.tables = {}
        self.foreign_tables = {}
        self.__dict__.update(orm.init.init_the_database(self, self.name, self.db_type, self.init_kwargs).__dict__)
        
    def cursor(self, auto_commit=1):
        return Cursor(self, auto_commit)
        
    def get_connection(self):
        if self.in_memory and self.last_connection:
            return self.last_connection

        self.last_connection = self.db_connector.create_connection()
        return self.last_connection
    
    def return_connection(self, conn):
        self.db_connector.return_connection(conn)
    
    def get_table(self, name, depth=0):
        if name in self.tables:
            return self.tables[name]
        elif (depth < 5) and (name in self.foreign_tables):
            return self.foreign_tables[name].get_table(name, depth=depth+1)
        elif (depth < 5):
            for i in self.foreign_databases:
                tmp = i.get_table(name)
                if tmp:
                    return tmp
                else:
                    pass
        else:
            return None
    
    def get_tables(self, prefix):
        ret = []
        for i in self.tables:
            if self.tables[i].table_name.startswith(prefix):
                ret.append(self.tables[i])
        return ret

    def calc_refs(self):
        for i in self.tables.keys():
            t = self.tables[i]
            missed = []

            for j in t.foreign_values.keys():
                if self.tables.has_key(t.foreign_values[j]):
                    x = (t.table_name, j)
                    if x not in self.tables[t.foreign_values[j]].refer_to_me:
                        self.tables[t.foreign_values[j]].refer_to_me.append((t.table_name, j))
                    else:
                        pass
                else:
                    missed.append(t.foreign_values[j])

            if missed:
                print "MISSED: %s" % str(missed)

    def class_updates(self):
        import orm.row
        
        for j in orm.row.SQLRow.__subclasses__():
            if not getattr(j, 'table', None):
                print 'No table class attribute made for %s' % j
                continue
        
            orm.row.SQLRow.row_objects[j.table] = j
            table = self.get_table(j.table)
            if table:
                if getattr(j, 'types', None):
                    x = getattr(j, 'types')
                    for i in x:
                        if i in table.column_types:
                            #print '\tchanging column type for %s to %s' % (i, str(x[i]))
                            table.column_types[i] = x[i]
                        else:
                            #print "\tCan't set type of %s for table %s because that column does not exist." % (i, j.table)
                            pass
                        
                if getattr(j, 'fkeys', None):
                    x = getattr(j, 'fkeys')
                    for i in x:
                        if i in table.foreign_values:
                            if x[i] == None:
                                #print '\tremoving fkey for %s' % i
                                table.foreign_values.pop(i)
                            else:
                                #print '\tadding foreign key for %s to %s' % (i, str(x[i]))
                                table.foreign_values[i] = x[i]
                        else:
                            #print "\tCant add foreign key %s for %s because that column does not exist." % (i, j.table)
                            pass

    def drop_tables(self, prefix, cursor=None):
        removed = []
        for i in self.tables:
            if i.startswith(prefix):
                self.tables[i].drop_table(cursor=cursor)
                removed.append(i)
        
        for i in removed:
            self.tables.pop(i)
    
    def drop_all_tables(self):
        self.reload()
        removed = []
        for i in self.tables:
            self.tables[i].drop_table()
            removed.append(i)
        
        for i in removed:
            self.tables.pop(i)
            
    def has_no_tables(self):
        self.reload()
        return self.tables == {}
            
    def dump_tables(self, tables):
        import tempfile
        
        try:
            os.mkdir('/tmp/pyjitsu_sql_dump/')
        except:
            pass

        filename = tempfile.mktemp()    
        print "Saving row data in %s." % filename
    
        f = open(filename, 'w')
    
        for i in tables:
            tmp = orm.get_table(i).dump_row_values()
            if tmp:
                f.write(tmp + '\n')
            f.flush()
        f.close()
        
        return filename
    
    def safe_table_order(self, prefix=None):
        checking_list = []

        nothing_count = 0
        
        while nothing_count < 10:
            nothing_count += 1
            
            for i in self.tables:
                table = self.tables[i]
                if i in checking_list:
                    pass
                else:
                    if table.foreign_values == {} and table.table_name not in checking_list:
                        checking_list.append(i)
                        nothing_count = 0
                    else:
                        missing = 0
                        for j in table.foreign_values:
                            target_table_name = table.foreign_values[j]

                            if target_table_name not in self.tables and target_table_name not in checking_list:
                                checking_list.append(target_table_name)
                            elif target_table_name not in checking_list:
                                missing = 1

                        if not missing:
                            checking_list.append(i)

        return_list = []
        
        if prefix:
            checking_list.reverse()
            for i in checking_list:
                if i.startswith(prefix):
                    return_list.append(i)
                    
            return_list.reverse()
        else:
            return_list = checking_list
            
        return return_list

    def load_data_file(self, files, loaded_tables=None):
        data = ''
        for i in files:
            data += open(i).read()
            data += '\n'
        
        dict_data = {}
        for i in data.split('\n'):
            if i:
                table_name, d_value = i.split('\t')
        
                if table_name in dict_data:
                    dict_data[table_name].append(eval(d_value))
                else:
                    dict_data[table_name] = [eval(d_value)]
        
        rows_created = 0
        for i in loaded_tables:
            if i in dict_data:
                table = orm.get_table(i)
                for j in dict_data[i]:
                    table.create(**j).save()
                    rows_created += 1
            
                update_sequence(table)
        return rows_created

    def load_sql_files(self, path):
        import os
        found_files = os.listdir(path)
        files = []
        
        loaded_tables = []
        
        for i in found_files:
            if i.endswith('.sql'):
                files.append(i)
                
        files.sort()
        
        for i in files:
            data = open(path+i).read()
            for j in data.split(';'):
                try:
                    if j.strip():
                        cursor = self.cursor()
                        cursor.execute(j)
                        cursor.conn.commit()
            
                        if j.strip().startswith('create'):
                            tmp_table_name = j.strip().split('\n')[0].rstrip('(').strip()
                            tmp_table_name = tmp_table_name.split(' ', 2)[-1]
                            loaded_tables.append(tmp_table_name.strip())
                except:
                    traceback.print_exc()
        return loaded_tables

    def add_foreign_table(self, db, table):
        self.foreign_tables[table] = db