import re
import orm

class ConditionNULL(object): pass

class ConditionBuilder(object):
    def __init__(self, table):
        self.table = table.table_name
        self.database = table.database
        self._pk = None
        self.columns = []
        self.collections = {
            'AND':SQLAnd,
            'OR':SQLOr
            }

    def __getattr__(self, key):
        if key in self.columns:
            return SQLColumn(self.database, self.table, key)
        elif key == 'primary_key' or key == 'pkey' or key == 'pk':
            return SQLColumn(self.database, self.table, self._pk)
        elif self.collections.has_key(key):
            return self.collections[key]()
        else:
            raise Exception("No such column: %s" % key)

class AnonymousBuilder(object):
    def __init__(self, db, dc):
        self.database = db
        self.connection_type = dc
        
    def __getattr__(self, key):
        if key == 'AND':
            return SQLAnd()
        elif key == 'OR':
            return SQLOr()
        else:
            return SQLColumn(orm.default_database or self.database, None, key)

class SQLColumn(object):
    def __init__(self, database, table, column):
        self.database = database
        self.connection_type = database.db_type
        
        if self.connection_type == 'bigtable':
            self.table = None
        else:
            self.table = table
    
        self.column = column
        self.match = {
            'equals':SQLEquals,
            'bool':SQLBool,
            'like':SQLLike,
            'ilike':SQLILike,
            'startswith':SQLStartsWith,
            'endswith':SQLEndsWith,
            'contains':SQLContains,
            'icontains':SQLIContains,
            'greaterthan':SQLGreaterThan,
            'lessthan':SQLLessThan,
            'greaterthanorequal':SQLGreaterThanOrEqual,
            'lessthanorequal':SQLLessThanOrEqual,
            'null':SQLNull,
            'none':SQLNull,
            'notnull':SQLNotNull,
            'notnone':SQLNotNull,
            'false':SQLFalse,
            'true':SQLTrue,
            'notfalse':SQLNotFalse,
            'nottrue':SQLNotTrue
            }
        
    def __getattr__(self, key):
        if self.match.has_key(key):
            return self.match[key](self.database, self.connection_type, self.table, self.column)
        else:
            raise Exception("No such sql condtion: %s" % key)

class SQLCondition(object):
    def __init__(self, database, connection_type, table, column):
        self.database = database
        self.connection_type = connection_type
        self.table = table
        self.column = column

    def __call__(self, value=None):
        self.value = value
        return self

    def build(self, table=None):
        return ''

class SQLEquals(SQLCondition):
    def build(self, table=None):
        if table:
            self.table = table
        part = '"%s".' % self.table
        part2 = '"%s"=%s' % (self.column, self.database.db_connector.escape_character())
        if self.table:
            return part + part2, [self.value]
        else:
            return part2, [self.value]
            
class SQLBool(SQLCondition):
    def build(self, table=None):
        if table:
            self.table = table
        part = '"%s".' % self.table
        part2 = '"%s"=%s' % (self.column, self.database.db_connector.escape_character())
        if self.table:
            return part + part2, [self.value]
        else:
            return part2, [self.value]

class SQLLike(SQLCondition):
    def build(self, table=None):
        if table:
            self.table = table
        part = '"%s".' % self.table
        part2 = '"%s" like %s' % (self.column, self.database.db_connector.escape_character()) 
        if self.table:
            return part + part2, [self.value]
        else:
            return part2, [self.value]

class SQLILike(SQLCondition):
    def build(self, table=None):
        if table:
            self.table = table
        part = '"%s".' % self.table
        part2 = '"%s" ilike %s' % (self.column, self.database.db_connector.escape_character()) 
        if self.table:
            return part + part2, [self.value]
        else:
            return part2, [self.value]

class SQLStartsWith(SQLCondition):
    def build(self, table=None):
        if table:
            self.table = table
        part = '"%s".' % self.table
        part2 = '"%s" like \'%%%s\'' % (self.column, self.database.db_connector.escape_character()) 
        if self.table:
            return part + part2, [self.value]
        else:
            return part2, [self.value]

class SQLEndsWith(SQLCondition):
    def build(self, table=None):
        if table:
            self.table = table
        part = '"%s".' % self.table
        part2 = '"%s" like \'%s%%\'' % (self.column, self.database.db_connector.escape_character()) 
        if self.table:
            return part + part2, [self.value]
        else:
            return part2, [self.value]

class SQLContains(SQLCondition):
    def build(self, table=None):
        if table:
            self.table = table
        part = '"%s".' % self.table
        part2 = "\"%s\" like '%%" % (self.column)
        part2 += re.escape(self.value)
        part2 += "%'"

        if self.table:
            return part + part2, []
        else:
            return part2, []

class SQLIContains(SQLCondition):
    def build(self, table=None):
        if table:
            self.table = table
        part = '"%s".' % self.table
        part2 = "\"%s\" ilike '%%" % (self.column)
        part2 += re.escape(self.value)
        part2 += "%'"

        if self.table:
            return part + part2, []
        else:
            return part2, []


class SQLGreaterThan(SQLCondition):
    def build(self, table=None):
        if table:
            self.table = table
        part = '"%s".' % self.table
        part2 = '"%s" > %s' % (self.column, self.database.db_connector.escape_character()) 
        if self.table:
            return part + part2, [self.value]
        else:
            return part2, [self.value]

class SQLLessThan(SQLCondition):
    def build(self, table=None):
        if table:
            self.table = table
        part = '"%s".' % self.table
        part2 = '"%s" < %s' % (self.column, self.database.db_connector.escape_character()) 
        if self.table:
            return part + part2, [self.value]
        else:
            return part2, [self.value]

class SQLGreaterThanOrEqual(SQLCondition):
    def build(self, table=None):
        if table:
            self.table = table
        part = '"%s".' % self.table
        part2 = '"%s" >= %s' % (self.column, self.database.db_connector.escape_character()) 
        if self.table:
            return part + part2, [self.value]
        else:
            return part2, [self.value]

class SQLLessThanOrEqual(SQLCondition):
    def build(self, table=None):
        if table:
            self.table = table
        part = '"%s".' % self.table
        part2 = '"%s" <= %s' % (self.column, self.database.db_connector.escape_character()) 
        if self.table:
            return part + part2, [self.value]
        else:
            return part2, [self.value]


class SQLNull(SQLCondition):
    def build(self, table=None):
        self.value = ConditionNULL
        if table:
            self.table = table
        part = '"%s".' % self.table
        part2 = '"%s" is null' % (self.column)
        if self.table:
            return part + part2, []
        else:
            return part2, []

class SQLNotNull(SQLCondition):
    def build(self, table=None):
        self.value = ConditionNULL
        if table:
            self.table = table
        part = '"%s".' % self.table
        part2 = '"%s" is not null' % (self.column)
        if self.table:
            return part + part2, []
        else:
            return part2, []

class SQLNotFalse(SQLCondition):
    def build(self, table=None):
        if table:
            self.table = table
        part = '"%s".' % self.table
        part2 = ''
        args = []
        
        if self.connection_type == 'sqlite':
            part2 = '"%s"!=%s' % (self.column, self.database.db_connector.escape_character())
            args.append(False)
        elif self.connection_type == 'postgres':
            part2 = '"%s" is NOT FALSE' % (self.column) 
            
        if self.table:
            return part + part2, args
        else:
            return part2, args
            
class SQLFalse(SQLCondition):
    def build(self, table=None):
        if table:
            self.table = table
        part = '"%s".' % self.table
        part2 = ''
        args = []
        if self.connection_type == 'sqlite':
            part2 = '"%s"=%s' % (self.column, self.database.db_connector.escape_character())
            args.append(False)
        elif self.connection_type == 'postgres':
            part2 = '"%s" is FALSE' % (self.column) 
            
        if self.table:
            return part + part2, args
        else:
            return part2, args

class SQLNotTrue(SQLCondition):
    def build(self, table=None):
        if table:
            self.table = table
        part = '"%s".' % self.table
        part2 = ''
        args = []
        if self.connection_type == 'sqlite':
            part2 = '"%s"!=%s' % (self.column, self.database.db_connector.escape_character())
            args.append(True)
        elif self.connection_type == 'postgres':
            part2 = '"%s" is NOT TRUE' % (self.column) 
            
        if self.table:
            return part + part2, args
        else:
            return part2, args

class SQLTrue(SQLCondition):
    def build(self, table=None):
        if table:
            self.table = table
        part = '"%s".' % self.table
        part2 = ''
        args = []
        if self.connection_type == 'sqlite':
            part2 = '"%s"=%s' % (self.column, self.database.db_connector.escape_character())
            args.append(True)
        elif self.connection_type == 'postgres':
            part2 = '"%s" is TRUE' % (self.column)
            
        if self.table:
            return part + part2, args
        else:
            return part2, args


class SQLCollection(object):
    def __init__(self):
        self.joiner = ' WOOT '

    def __call__(self, *args):
        self.items = []

        for i in args:
            if i:
                self.items.append(i.build())

        return self

    def build(self):
        q = []
        vars = []
        for query, var in self.items:
            q.append(query)
            vars += var
        return '(' + self.joiner.join(q) + ')', vars

class SQLAnd(SQLCollection):
    def __init__(self):
        SQLCollection.__init__(self)
        self.joiner = ' AND '

class SQLOr(SQLCollection):
    def __init__(self):
        SQLCollection.__init__(self)
        self.joiner = ' OR '

