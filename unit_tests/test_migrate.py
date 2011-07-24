import unittest
import time
import orm
import orm.migrate

from orm.unit_tests.my_test import Test_MyTest

class Test_MigrateData(Test_MyTest):
    def test_save_data_load_data(self):
        self.db.create_table('foo', 'id serial primary key, name varchar(128), age int')
        self.db.create_table('bar', 'id serial primary key, name varchar(128), age int')
        self.db.create_table('baz', 'id serial primary key, name varchar(128), age int')
        
        self.db.reload()
        
        for i in range(1, 11):
            self.db.foo.create(name='my name is %s' % i, age=i*5).save()
            self.db.bar.create(name='my name is %s' % i, age=i*15).save()
            self.db.baz.create(name='my name is %s' % i, age=i*25).save()

        filename = orm.migrate.save_data(self.db, self.db.tables.keys())
        
        for i in self.db.tables:
            self.db.tables[i].delete()
            
        assert self.db.foo.count() == 0
        assert self.db.bar.count() == 0
        assert self.db.baz.count() == 0
        
        self.db.drop_all_tables()
        self.db.reload()
        
        self.db.create_table('foo', 'id serial primary key, name varchar(128), age int')
        self.db.create_table('bar', 'id serial primary key, name varchar(128), age int')
        self.db.create_table('baz', 'id serial primary key, name varchar(128), age int')
        
        self.db.reload()

        orm.migrate.load_data(self.db, filename)
        
        assert self.db.foo.count() == 10
        assert self.db.bar.count() == 10
        assert self.db.baz.count() == 10

        for i in self.db.foo.find(order='id'):
            assert i.name == 'my name is %i' % i.id
            assert i.age == i.id * 5
        
        for i in self.db.bar.find(order='id'):
            assert i.name == 'my name is %i' % i.id
            assert i.age == i.id * 15
        
        for i in self.db.baz.find(order='id'):
            assert i.name == 'my name is %i' % i.id
            assert i.age == i.id * 25
            
    def test_safe_table_order(self):
        self.db.create_table('foo', 'id serial primary key, name varchar(128)')
        self.db.create_table('bar', 'id serial primary key, name varchar(128)')
        self.db.create_table('baz', 'id serial primary key, \
                                     rfoo integer references foo(id), \
                                     rbar integer references bar(id)')
        self.db.create_table('boo', 'id serial primary key, rbaz integer references baz(id)')
        self.db.create_table('poo', 'id serial primary key, \
                                     rboo integer references boo(id), \
                                     rfoo integer references foo(id)')
        self.db.reload()
        
        assert self.db.safe_table_order() == ['foo', 'bar', 'baz', 'boo', 'poo']
        
    