from cursor import Cursor

sql_prefix = {
    'select':'SELECT %s FROM %s',
    'delete':'DELETE FROM %s',
    'update':'UPDATE %s SET',
    'insert':'INSERT INTO %s',
    'count':'SELECT count(*) FROM %s'
}

class SQLQuery(object):
    def __init__(self, database, table_name, qtype='select', columns=[], where=''):
        self.database = database
        self.table_name = table_name
        self.query_type = qtype
        self.where = where
        self.offset = ''
        self.limit = ''
        self.order = ''
        self.columns = columns
        self.query_string = ''
        self.vars = []

    def __repr__(self):
        return "<DB: %s Table: %s Type: %s Cond: %s>" % (self.database, self.table_name, self.query_type, self.where)

    def build(self, vars):
        self.query_string = ''
        if self.vars:
            pass
        else:
            self.vars += vars
        prefix = ''
        conditions = ''
        quoted_cols = []

        if self.columns == '*':
            quoted_cols = ['*']
        else:
            for i in self.columns:
                if not i.startswith('"'):
                    quoted_cols.append('"%s"."%s"' % (self.table_name, i))
                else:
                    quoted_cols.append(i)
        
        center = ''
        if self.query_type == 'select':
            prefix = sql_prefix[self.query_type] % (','.join(quoted_cols), self.table_name)
            center = ''
        elif self.query_type == 'count':
            prefix = sql_prefix[self.query_type] % (self.table_name)
            center = ''
        elif self.query_type == 'delete':
            prefix = sql_prefix[self.query_type] % self.table_name
            center = ''
        elif self.query_type == 'update':
            prefix = sql_prefix[self.query_type] % self.table_name
            q = []
            for index, obj in enumerate(columns):
                q.append(('%s=' % quoted_cols[index]) + self.database.db_connector.escape_character())
                self.vars.append(self.columns[obj])          
        elif self.query_type == 'insert':
            self.where = None
            self.order = None
            self.limit = None
            self.offset = None
            prefix = sql_prefix[self.query_type] % self.table_name
            q = ['%s' for i in range(0, len(quoted_cols))]
            center = '(%s) values(%s)' % (','.join(quoted_cols), ','.join(q))
            self.vars = self.columns.values()
        
        if self.where:
            conditions += ' WHERE %s ' % (self.where)
        if self.order:
            conditions += ' ORDER BY %s ' % (self.order)
        if self.limit:
            conditions += ' LIMIT %i ' % int(self.limit)
        if self.offset:
            conditions += ' OFFSET %i ' % int(self.offset)

        self.query_string = '%s %s %s' % (prefix, center, conditions)
        self.query_string = self.query_string.strip() + ';'
        return self.query_string

    def run(self, cursor=None):
        if not self.query_string:
            self.build()

        if cursor == None:
            cursor = self.database.cursor()
            
        cursor.execute(self.query_string, self.vars)
        if self.query_type == 'select':
            data = cursor.fetchall()
            return data
        elif self.query_type == 'count':
            data = cursor.fetchone()
            return data.values()[0]
        else:
            return None
