from orm.conn_types import Connection
from orm.util import SQLNone
import orm
import os
import threading

class PostgresConnection(Connection):
    def __init__(self, database, **kwargs):
        self.connection_pool = {}
        self.database = database
        self.username = os.getlogin()
        self.host = 'localhost'

        self.kwargs = kwargs

        for i in kwargs:
            setattr(self, i, kwargs[i])
            
    def return_connection(self, connection):
        curr_thread = threading.currentThread()
        self.connection_pool[curr_thread] = connection
        
    def create_connection(self):
        import psycopg2
    
        psycopg2.paramstyle = "qmark"
        connect_string = "dbname='%s' user='%s'" % (self.database, self.username)
    
        for i in self.kwargs:
            connect_string += " %s='%s'" % (i, self.kwargs[i])
    
        conn = psycopg2.connect(connect_string)
        
        self.connection_pool[curr_thread] = conn
        
        return conn
    
    def translate(self, txt):
        return txt
    
    def escape_character(self):
        return '%s'
        
    def save_row(self, row, cursor, silent, reflect):
        q = "INSERT INTO \"%s\"(%s) values("

        newvals = row.values.keys()
        for i in row.values.keys():
            if row.values[i].__class__ == SQLNone:
                newvals.remove(i)

        vals = []
        changed_columns = []
        for i in newvals:
            v = row.table.column_types[i].pre_save(row.values[i])
            changed_columns.append(i)
            vals.append(v)

        ad = ', '.join( [row.database.db_connector.escape_character() for i in range(0, len(vals))]) + ');'

        quoted_cols = []
        for i in changed_columns:
            if not i.startswith('"'):
                quoted_cols.append('"%s"' % i)
            else:
                quoted_cols.append(i)

        cursor.execute(q % (row.table.table_name, ','.join(quoted_cols)) + ad, vars=vals, log_query=silent)

        if reflect:
            if row.table.primary_key in changed_columns:
                if orm.verbose_level() > 1:
                    print "\tPrimary Key was set in commit, so skipping."
            else:
                if row.table.primary_key:
                    cursor.execute("select CURRVAL('%s_%s_seq');" % (row.table.table_name, row.table.primary_key), log_query=silent)
                    row.values[row.table.primary_key] = cursor.fetchone()['currval']
                    row.original_values[row.table.primary_key] = row.values[row.table.primary_key]
