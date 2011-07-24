from cursor import Cursor
import table

FIRST_TABLE_CHAR = '-'

class MultiQuery(object):
    def __init__(self, database, primary_table, eager_join=[], where='', vars=[]): 
        self.database = database
        self.primary_table = primary_table
        if type(eager_join) == str:
            self.eager_join = [eager_join]
        else:
            self.eager_join = eager_join
        self.add_conditions = where
        self.vars = vars
        self.query_string = None
        self.limit = 0
        self.order = ''
        self.offset = 0

    def table_rename(self, table, key):
        x = []
        for i in table.columns:
            x.append('%s.%s as "%s.%s.%s"' % (table.table_name, i, table.table_name, key, i))
        return ', '.join(x)

    def build(self, vars=[]):
        self.vars = vars

        names = []
        tables = []
        conditions = []
        names.append(self.table_rename(self.primary_table, FIRST_TABLE_CHAR))
        tables.append(self.primary_table.table_name)

        for i in self.eager_join:
            for_table = self.database.get_table(self.primary_table.foreign_values[i])
            names.append(self.table_rename(for_table, i))
            if for_table.table_name not in tables:
                tables.append(for_table.table_name)
            conditions.append( '%s.%s = %s.%s' % (self.primary_table.table_name, i, for_table.table_name, for_table.primary_key) )

        q = 'SELECT %s ' % ', '.join(names)
        q += ' FROM %s ' % ', '.join(tables)

        if not self.add_conditions:
            pass
        elif type(self.add_conditions) == str:
            if self.add_conditions:
                conditions.append(self.add_conditions)
        else:
            ccc, vvv = self.add_conditions.build()
            conditions.append(ccc)
            self.vars = vvv
            

        if conditions:
            q += ' WHERE ' + ' AND '.join(conditions)

        if self.order:
            q += ' ORDER BY %s ' % (self.order)
        if self.limit:
            q += ' LIMIT %i ' % int(self.limit)
        if self.offset:
            q += ' OFFSET %i ' % int(self.offset)

        self.query_string = q

    def run(self, cursor=None):
        q = self.query_string
        if self.query_string == None:
            self.build()

        x = cursor
        if not cursor:
            x = self.database.cursor()
            
        x.execute(q, self.vars)
        r = x.fetchall()
        results = []

        for row in r:
            main_values = {}
            foreign_data = {}

            for i in self.eager_join:
                foreign_data[i] = {}

            foreign_tables = {}

            for key in row:
                table_name, field, column = key.split('.')
                if field == FIRST_TABLE_CHAR:
                    main_values[column] = row[key]
                else:
                    foreign_tables[field] = table_name
                    foreign_data[field][column] = row[key]

            main_obj = self.primary_table.build_row(main_values)
            
            for i in foreign_tables:
                t = self.database.get_table(foreign_tables[i])
                main_obj.foreign_data[i] = t.build_row(foreign_data[i])
            
            results.append(main_obj)
                
        return results
            
