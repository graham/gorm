import os
import tempfile

import orm

def save_data(db, tables, verbose=0):
    filename = tempfile.mktemp()    
    f = open(filename, 'w')
    
    for i in tables:
        if type(i) == str:
            table = db.get_table(i)
        else: # type(i) == orm.table.SQLTable
            table = i
        
        if verbose:
            print "Saving Data for %s: " % table.table_name,
            
        tmp = table.dump_row_values()
        
        if verbose:
            print "%i rows." % tmp.count('\n')
            
        if tmp:
            f.write(tmp + '\n')
            
    f.flush()
    f.close()
        
    return filename
    
def load_data(db, filename, prefix=None, verbose=0):
    data = open(filename).read()
    
    dict_data = {}
    for i in data.split('\n'):
        if i:
            table_name, d_value = i.split('\t')
        
            if table_name in dict_data:
                dict_data[table_name].append(eval(d_value))
            else:
                dict_data[table_name] = [eval(d_value)]

    rows_created = 0
    
    loaded_tables = db.safe_table_order(prefix)

    for i in loaded_tables:
        if i in dict_data:
            table = db.get_table(i)
            if not table:
                print 'Skipping %s because table does not exist.'
                continue
                
            table_count = 0
            if verbose:
                print '  Creating rows in %s: ' % table.table_name,

            for j in dict_data[i]:
                table.create(**j).save()
                rows_created += 1
                table_count += 1
                
            if verbose:
                print table_count
                
            table.safe_update_sequence()
            
    return rows_created

def easy_migrate(db, prefix, path):
    filename = save_data(db, db.get_tables(prefix))
    
    raw_input("Hit enter to drop tables.")
    
    order = db.safe_table_order(prefix)
    order.reverse()
    print "Dropping Tables: ",
    for i in order:
        i = db.get_table(i)
        if i:
            print '%s, ' % i.table_name,
            i.drop_table()
    
    raw_input("Hit enter to start loading tables.")
    print '\n'
    
    print "Loading Tables: ",
    for i in db.load_sql_files(path):
        print "%s, " % i, 
        
    print '\n'
        
    db.reload()

    print "Cleaning Tables: ",
    for i in db.get_tables(prefix):
        print '%s, ' % i.table_name,
        i.delete()
        
    print '\n'
     
    count = load_data(db, filename, prefix, verbose=1)
    
    
    print "\nLoaded %i rows." % count
    
def easy_migrate_silent(db, prefix, path):
    filename = save_data(db, db.get_tables(prefix))
    order = db.safe_table_order(prefix)
    order.reverse()

    for i in order:
        i = db.get_table(i)
        if i:
            i.drop_table()
    
    for i in db.load_sql_files(path):
        pass
        
    db.reload()

    for i in db.get_tables(prefix):
        i.delete()
        
    count = load_data(db, filename, prefix, verbose=1)
    
    
    return count