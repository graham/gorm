import unittest
from orm.unit_tests.my_test import Test_MyTest
import orm

class Test_CreateMemoryDatabase(Test_MyTest):
    def setUp(self):
        self.db = orm.memory_db('foo')
        assert self.db.has_no_tables() == True
        
    def test_create_database(self):
        assert issubclass(self.db.__class__, orm.database.SQLDatabase) == True
        assert self.db.tables == {}
        
    def test_memory_database_one_connection(self):
        assert self.db.get_connection() == self.db.get_connection(), \
            'Memory database using different connections.'
    
    def test_database_see_table(self):
        self.db.create_table("foo", "value int, name varchar(128)")
        self.db.reload()
        assert 'foo' in self.db.tables
        assert issubclass(self.db.get_table('foo').__class__, orm.table.SQLTable) == True
        
    def test_database_see_row(self):
        self.db.create_table('foo', 'value int, name varchar(128)')
        self.db.reload()
        x = self.db.foo.create()
        x.value = 10
        x.name = 'orm'
        x.save()
        
        y = self.db.foo.find_one()
        assert y.values == x.values
    
    def tearDown(self):
        self.db.drop_all_tables()
        assert self.db.has_no_tables()

