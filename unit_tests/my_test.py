import unittest
import time
import orm

import orm.unit_tests
import sys

if orm.unit_tests.db_type == 'sqlite':
    my_db = orm.temp_db('foo')
elif orm.unit_tests.db_type == 'postgres':
    my_db = orm.quick_load('postgres', host='127.0.0.1', user='graham', database='orm_test_db')
else:
    my_db = orm.temp_db('foo')
        
class Test_MyTest(unittest.TestCase):
    def setUp(self):
        self.db = my_db
        assert self.db.has_no_tables() == True
                
    def tearDown(self):
        self.db.drop_all_tables()
        assert self.db.has_no_tables() == True

