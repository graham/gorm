from orm.row import SQLRow
from orm.condition import ConditionBuilder
from orm.query import SQLQuery
from orm.multiquery import MultiQuery

class SQLTable(object):
    def __init__(self, database, table_name, primary_key=None, columns=[], foreign_values={}, column_types={}):
        self.database = database
        self.table_name = table_name
        self.primary_key = primary_key
        self.columns = columns
        self.foreign_values = foreign_values
        self.column_types = column_types
        self._count = -1
        self.refer_to_me = []
        
        self.q = ConditionBuilder(self)
        self.q._pk = self.primary_key
        self.q.columns = self.columns

        self.defaults = {}
        self.schema = ''
        
        self.log_table = None

    def drop_table(self, cursor = None):
        if cursor == None:
            cursor = self.database.cursor()
        try:
            cursor.execute("drop table %s CASCADE" % self.table_name)
        except:
            pass

    def create(self, **kwargs):
        r = self.build_row({})
        r.update(**self.defaults)
        r.update(**kwargs)
        return r
            
    def get_row_object(self):
        if SQLRow.row_objects.has_key(self.table_name):
            return SQLRow.row_objects[self.table_name]
        else:
            return SQLRow
        
    def build_row(self, values, **kwargs):
        if SQLRow.row_objects.has_key(self.table_name):
            return SQLRow.row_objects[self.table_name](database=self.database, table=self, values=values, **kwargs)
        else:
            return SQLRow(database=self.database, table=self, values=values, **kwargs)

    def build_rows(self, list_response, **kwargs):
        a = []
        count = 0
        for i in list_response:
            r = self.build_row(i, **kwargs)
            r.query_index = count
            count += 1
            a.append(r)
        return a

    def build_query_object(self, only=None, exclude=None):
        columns = []
        if exclude:
            columns = [i for i in self.columns]
            for i in exclude:
                columns.remove(i)
            return SQLQuery(self.database, self.table_name, 'select', columns)
        elif only:
            return SQLQuery(self.database, self.table_name, 'select', only)
        else:
            return SQLQuery(self.database, self.table_name, 'select', '*')

    def query(self, query, vars, cursor=None):
        return self.result_from_query(query, vars, cursor)
        
    def result_from_query(self, query, vars, cursor=None):
        if cursor == None:
            cursor = self.database.cursor()
        
        cursor.execute(query, vars)
        return self.build_rows(cursor.fetchall())
    
    def glue_result_from_query(self, joined_table, target_table, query, vars, cursor=None):
        if cursor == None:
            cursor = self.database.cursor()
        
        cursor.execute(query, vars)
        rows = cursor.fetchall()
        
        return self.build_rows(rows, joined_table=joined_table, target_table=target_table)
        
    def get(self, id, only=None, exclude=None, cursor=None):
        if id == None:
            return None
        try:
            id = self.column_types[self.primary_key].py_type(id)
        except:
            pass
            
        q = self.build_query_object(only=only, exclude=exclude)
        c = self.q.primary_key.equals(id)
        q.where = c.build()[0]
        q.build(c.build()[1])
        result = q.run(cursor)
        if not result:
            return None
        else:
            return self.build_row(result[0])        
    
    def find(self, condition=None, order='', offset=0, limit=0, only=None, exclude=None, cursor=None):
        if not condition:
            cwhere = []
            cvars = []
        else:
            cwhere, cvars = condition.build()
        query_obj = self.build_query_object(only=only, exclude=exclude)
        query_obj.where = cwhere

        if order:
            query_obj.order = order
        if offset:
            query_obj.offset = offset
        if limit:
            query_obj.limit = limit
        query_obj.build(cvars)
        return self.build_rows(query_obj.run(cursor))
    
    def match(self, limit=0, offset=0, only=None, exclude=None, cursor=None, **kwargs):
        order_by = ''
        if 'order' in kwargs:
            order_by = kwargs.pop('order')
        k = []
        for i in kwargs:
            if issubclass(kwargs[i].__class__, SQLRow):
                id = kwargs[i].get_primary()
                if id:
                    kwargs[i] = id
        
        for i in kwargs:
            val = kwargs[i]
            if val == None:
                k.append(getattr(self.q, i).null())
            else:
                k.append(getattr(self.q, i).equals(val))
                
        query_obj = self.build_query_object(only=only, exclude=exclude)
        vars = []
        query_obj.where, vars = self.q.AND( *k ).build()
        query_obj.order = order_by
        query_obj.limit = limit
        query_obj.offset = offset
        query_obj.build(vars)
        return self.build_rows(query_obj.run(cursor))        

    def delete(self, condition=None, cursor=None):
        if not condition:
            where = []
            vars = []
        else:
            where, vars = condition.build()
        query_obj = SQLQuery(self.database, self.table_name, 'delete', where=where)
        query_obj.build(vars)
        query_obj.run(cursor)
        
    def count(self, condition=None, cursor=None, **kwargs):
        if not condition:
            where = []
            vars = []
        else:
            where, vars = condition.build()
        query_obj = SQLQuery(self.database, self.table_name, 'count', where=where)
        query_obj.build(vars)
        result = query_obj.run(cursor)
        return result
        
    def eager(self, join=[], condition=None, order='', offset=0, limit=0, cursor=None):
        if not condition:
            where = []
            vars = []
        else:
            where, vars = condition.build()
        query_obj = MultiQuery(self.database,self, eager_join=join, where=where)
        if order:
            query_obj.order = order
        if offset:
            query_obj.offset = offset
        if limit:
            query_obj.limit = limit
        query_obj.build(vars)
        return query_obj.run(cursor)
        
    def find_one(self, *args, **kwargs):
        kwargs['limit'] = 1
        ret = self.find(*args, **kwargs)
        if len(ret) > 0:
            return ret[0]
        else:
            return None
        
    def match_one(self, **kwargs):
        kwargs['limit'] = 1
        ret = self.match(**kwargs)
        if len(ret) > 0:
            return ret[0]
        else:
            return None
        
    def eager_one(*args, **kwargs):
        kwargs['limit'] = 1
        return self.eager(*args, **kwargs)

    def all(self):
        return self.find(order='id')
            
    def schema_compare(self, alt_table):
        pass
        
    def dump_rows(self):
        import StringIO
        tmp = StringIO.StringIO()
        for i in self.find(order='id'):
            print >>tmp, i.as_sql()
        return tmp.getvalue()
        
    def dump_row_values(self):
        import StringIO
        tmp = StringIO.StringIO()
        for i in self.find(order='id'):
            print >>tmp, "%s\t%r" % (self.table_name, i.values)
        return tmp.getvalue()
    
    def set_sequence_value(self, value):
        if self.database.db_type == 'sqlite':
            pass
        elif self.database.db_type == 'postgres':
            cursor = self.database.cursor()
            cursor.execute("alter SEQUENCE %s_%s_seq start %i;" % (self.table_name, self.primary_key, value))
            cursor.execute("select CURRVAL('%s_%s_seq');" % (self.table_name, self.primary_key))
            return cursor.fetchall()[0]['currval']
        else:
            raise Exception("I dont know how to deal with database: %s" % self.database.db_type)
            
    def safe_update_sequence(self):
        biggest_id = self.find_one(order='id DESC')
        if biggest_id:
            new_value = biggest_id.id + 1
            self.set_sequence_value(new_value)
        return new_value
        