import unittest
import time
import orm

from orm.unit_tests.my_test import Test_MyTest

class Test_CreateTempDatabase(Test_MyTest):
    def test_cursor_test(self):
        self.db = orm.memory_db('my_test')
        
        self.db.create_table('foo', 'id serial primary key, name varchar(128)')
        self.db.reload()

        c = self.db.cursor()
        
        x = self.db.foo.create()
        x.name = 'graham'

        assert [] == self.db.foo.find()

        x.save(cursor=c)

        assert len(self.db.foo.find()) == 1
    
    def test_cursor_transactions(self):
        self.db = orm.temp_db('my_test')
        
        self.db.create_table('foo', 'id serial primary key, name varchar(128)')
        self.db.reload()

        c = self.db.cursor(auto_commit=0)
        c2 = self.db.cursor(auto_commit=0)
        
        x = self.db.foo.create()
        x.name = 'graham'

        assert [] == self.db.foo.find()

        x.save(cursor=c)

        assert len(self.db.foo.find(cursor=c)) == 1
        assert len(self.db.foo.find(cursor=c2)) == 0
        
        c.commit()
        
        assert len(self.db.foo.find(cursor=c)) == len(self.db.foo.find(cursor=c2))
