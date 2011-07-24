import threading

from orm.conn_types import Connection
from orm.util import SQLNone
import orm

class SQLiteConnection(Connection):
    def __init__(self, filename):
        self.filename = filename
        self.connection_pool = {}
        
    def return_connection(self, connection):
        curr_thread = threading.currentThread()
        self.connection_pool[curr_thread] = connection
        
    def create_connection(self):
        conn = None
        try:
            import sqlite3
        except:
            from pysqlite2 import dbapi2 as sqlite3

        conn = sqlite3.connect(self.filename)
        sqlite3.paramstyle = "qmark"
        conn.text_factory = str
        
        return conn

    def translate(self, data):
        if type(data) == str:
            data = data.replace('serial', 'integer')
            data = data.replace('bytea', 'blob')
            data = data.replace('now()', "date('now')")
            data = data.replace('False', "0")
            data = data.replace('True', "1")
            data = data.replace('CASCADE', '')
            return data
        else:
            return data
    
    def translate_data(self, data):
        if type(data) == str:
            data = data.replace('serial', 'integer')
            data = data.replace('bytea', 'blob')
            data = data.replace('now()', "date('now')")
            data = data.replace('False', "0")
            data = data.replace('True', "1")
            return data
        else:
            return data

    def escape_character(self):
        return '?'
        
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
                    row.values[row.table.primary_key] = cursor.cursor.lastrowid
                    row.original_values[row.table.primary_key] = row.values[row.table.primary_key]