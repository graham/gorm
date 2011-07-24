import orm
from orm.util import SQLNone

class Connection(object):
    
    def create_connection(self):
        return None
    
    def return_connection(self):
        pass
    
    def translate(self, txt):
        return txt
    
    def translate_data(self, data):
        return data

    def escape_character(self):
        return '%s'
        
    def delete_row(self, row, cascade):
        if cascade:
            for i in row.table.refer_to_me:
                t = row.database.get_table(i[0])
                #t.delete( getattr(t.q, i[1]).equals(row.get_primary()) )
                for j in t.find( getattr(t.q, i[1]).equals(row.get_primary()) ):
                    j.delete(cascade=1)
        
        where = []
        vars = []
        qu = orm.query.SQLQuery(row.database, row.table.table_name, 'delete')
        if row.get_primary():
            where, vars = row.table.q.primary_key.equals(row.get_primary()).build()
            qu.where = where
        else:
            where = []
            vars = row.values.values()
            
            for i in row.values.keys():
                if row.values[i] == None:
                    where.append('"%s"."%s" is null' % (row.table.table_name, i))
                else:
                    where.append('"%s"."%s"=%s' % (row.table.table_name, i, row.database.db_connector.escape_character()))
                    
            qu.where = ' AND '.join(where)
            
        qu.build(vars)
        qu.run()

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
                    if cursor.database.db_type == 'postgres':
                        if row.table.primary_key != None:
                            cursor.execute("select CURRVAL('%s_%s_seq');" % (row.table.table_name, row.table.primary_key), log_query=silent)
                            row.values[row.table.primary_key] = cursor.fetchone()['currval']
                            row.original_values[row.table.primary_key] = row.values[row.table.primary_key]
                    elif cursor.database.db_type == 'sqlite':
                        row.values[row.table.primary_key] = cursor.cursor.lastrowid
                        row.original_values[row.table.primary_key] = row.values[row.table.primary_key]
    
    def update_row(self, row, cursor, silent, reflect):
        if len(row.updated_values):
            q = "UPDATE %s SET " % row.table.table_name
            q +=  ', '.join( [('"%s"' % i)+'='+row.database.db_connector.escape_character() for i in row.updated_values.keys()] )
            if row.table.primary_key:
                q += " WHERE %s=" % row.table.primary_key
                q += "%s;" % row.database.db_connector.escape_character()
            else:
                q += ' WHERE ' + ' AND '.join( [('"%s"' % i)+'='+row.database.db_connector.escape_character() for i in row.original_values.keys()] )                        

            v = []
            for i in row.updated_values.keys():
                if i in row.table.column_types:
                    v.append(row.table.column_types[i].pre_save(row.values[i]))
            
            if row.table.primary_key:
                v.append(row.original_values[row.table.primary_key])
            else:
                v += row.original_values.values()

            cursor.execute( q, vars=v, log_query=silent)

                
import sqlite_conn
import postgres_conn
import mysql_conn
import bigtable_conn

connectors = {
    'sqlite'    :   sqlite_conn.SQLiteConnection,
    'postgres'  :   postgres_conn.PostgresConnection,
    'mysql'     :   mysql_conn.MySQLConnection,
    'bigtable'    :   bigtable_conn.BigTableConnection,
}

