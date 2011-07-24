import unittest
from orm.unit_tests.my_test import Test_MyTest
import orm

if orm.unit_tests.db_type == 'postgres':
    class Test_Postgres_Connect(Test_MyTest):
        def setUp(self):
            self.db = orm.quick_load('postgres', host='localhost', user='graham', database='orm_test_db')
            assert self.db.has_no_tables() == True
                
        def tearDown(self):
            self.db.drop_all_tables()
            assert self.db.has_no_tables() == True

        def test_create_database(self):
            assert issubclass(self.db.__class__, orm.database.SQLDatabase) == True
            assert self.db.tables == {}
        
        def test_create_database_with_reload(self):        
            self.db.create_table("foo", "value int, name varchar(128)")
            self.db.reload()

            y = orm.quick_load('postgres', host='localhost', user='graham', database='orm_test_db')

            assert 'foo' in self.db.tables
            assert 'foo' in y.tables
