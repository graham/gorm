import sys
import time
import traceback
import orm

normal =         "\033[0m"
cyan =           "\033[36m"

query_count = [0]

class Cursor(object):
    def __init__(self, database, auto_commit=1):
        self.auto_commit = auto_commit
        self.database = database
        
        self.conn = database.get_connection()
        self.cursor = self.conn.cursor()
        
    def execute(self, q, vars=[], log_query=1):
        query_count[0] += 1
        q = self.database.db_connector.translate(q)
        vars = map(self.database.db_connector.translate_data, vars)
        
        start = time.time()

        try:
            if vars:
                self.cursor.execute(q, vars)
            else:
                self.cursor.execute(q)
                        
            if self.auto_commit:
                self.conn.commit()

            end = time.time()

            if getattr(self.cursor, 'query', None):
                if orm.verbose_level() > 0:
                    print cyan, '%.5f' % (end-start), normal, '\t',  self.cursor.query
            else:
                if orm.verbose_level() > 0:
                    print cyan, '%.5f' % (end-start), normal, '\t',  q

        except Exception, e:
            self.conn.rollback()
            print >>sys.stderr, 'ERROR: ', q, vars
            traceback.print_exc()
            raise e

    def d_fetchall(self, data):
        colnames = [t[0] for t in self.cursor.description]  
        rows = [dict(zip(colnames, tup)) for tup in data]
        return rows

    def fetchall(self):
        if self.cursor.rowcount:
            res = self.cursor.fetchall()
        else:
            res = {}
            
        if res:
            if type(res[0]) == dict:
                return res
            else:
                return self.d_fetchall(res)
        else:
            return {}

    def fetchone(self):
        colnames = [t[0] for t in self.cursor.description]
        x = self.cursor.fetchone()
        if x:
            if type(x) == dict:
                return x
            else:
                return dict(zip(colnames, x))
        else:
            return {}
            
    def commit(self):
        try:
            start = time.time()
            self.conn.commit()
            end = time.time()
            if orm.verbose_level() > 0:
                print 'Commit: %.10f' % (end-start)
        except:
            import traceback
            traceback.print_exc()
            self.conn.rollback()

    
    def rollback(self):
        try:
            start = time.time()
            self.conn.rollback()
            end = time.time()
            if orm.verbose_level() > 0:
                print 'Rollback: %.10f' % (end-start)
        except:
            import traceback
            traceback.print_exc()
            self.conn.commit()

    def __del__(self):
        self.database.return_connection(self.conn)