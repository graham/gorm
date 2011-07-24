import sys
from orm.conn_types import Connection
import os

class BigTableConnection(Connection):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

        for i in kwargs:
            setattr(self, i, kwargs[i])
            
    def create_connection(self):
        return FakeBigTableConnection()
    
    def translate(self, txt):
        return txt.replace('"', '')
    
    def escape_character(self):
        return ':%s'
        
    def delete_row(self, row, cascade):
        row.values['_big_table_object'].delete()
        
    def save_row(self, row, cursor, silent, reflect):
        from google.appengine.ext import db
        import orm.util
        
        big_table_table = None
        
        for i in orm.util.get_all_subclasses(db.Model):
            if i.__name__ == row.table.table_name:
                big_table_table = i
        
        new_row = big_table_table(**row.values)
        for i in row.values:
            setattr(new_row, i, row.values[i])
        new_row.put()
                        
        if row.table.primary_key:
            row.values[row.table.primary_key] = new_row.key().id()
        
        row.values['_big_table_object'] = new_row
    
    def update_row(self, row, cursor, silent, reflect):
        bto = row.values['_big_table_object']
        for i in row.updated_values:
            setattr(bto, i, row.values[i])
        bto.save()
        
class FakeCursor(object):
    def __init__(self, *args, **kwargs):
        self.fetched = []
        
    def execute(self, q, vars=[]):
        from google.appengine.ext.db import GqlQuery
        q = q.strip(' ').strip(';')

        new_q = []
        for index, i in enumerate(q.split('%s')):
            if i:
                new_q.append(i)
                if index < q.count('%s'):
                    new_q.append(str(index+1))

        print >>sys.stderr, new_q
        
        q = ''.join(new_q)
        
        print >>sys.stderr, q

        
        if vars:
            self.pure_fetched = GqlQuery(q, vars)
        else:
            self.pure_fetched = GqlQuery(q)

        self.row_order = []

        if len(list(self.pure_fetched)) > 0:
            self.description = [ ('id', None, None, None, None, None, None)  ]
            for i in self.pure_fetched[0]._entity:
                self.row_order.append(i)
                self.description.append( (str(i), None, None, None, None, None, None) )
            self.description.append(('_big_table_object', None, None, None, None, None, None))
        else:
            self.description = []
            
        self.fetched = []
        for i in self.pure_fetched:
            l = [i.key().id()]
            for j in self.row_order:
                l.append( i._entity[j] )
            l.append(i)
            self.fetched.append(l)
        
    def rowcount(self):
        return len(list(self.fetched))
        
    def fetchall(self):
        tmp = self.fetched
        self.fetched = None
        return tmp
        
    def fetchone(self):
        tmp, self.fetched = self.fetched[:1], self.fetched[1:]
        return tmp
        
class FakeBigTableConnection(object):
    def __init__(self, *args, **kwargs):
        pass
        
    def cursor(self):
        return FakeCursor()
    
    def commit(self):
        pass
    def rollback(self):
        pass