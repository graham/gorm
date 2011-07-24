import unittest
from orm.unit_tests.my_test import Test_MyTest
import time
import orm

class Test_Rows(Test_MyTest):
    def test_create_row(self):
        self.db.create_table('foo', 'id serial primary key, name varchar(128)')
        self.db.reload()
        
        x = self.db.foo.create()
        x.name = 'graham'
        x.save()
        
        assert self.db.foo.count() == 1
        assert self.db.foo.get(1) == x
    
    def test_get_row(self):
        self.db.create_table('foo', 'id serial primary key, name varchar(128)')
        self.db.reload()
        
        x = self.db.foo.create()
        x.name = 'graham'
        x.save()
        
        assert x.id == self.db.foo.get(x.id).id
        
    def test_row_reload(self):
        self.db.create_table('foo', 'id serial primary key, name varchar(128)')
        self.db.reload()

        x = self.db.foo.create()
        x.name = 'graham'
        x.save()
        
        y = self.db.foo.get(x.id)
        y.name = 'foobar'
        y.save()
        
        assert x.name == 'graham'
        assert y.name == 'foobar'
        
        x.reload()
        
        assert x.name == y.name
    
    def test_get_primary(self):
        self.db.create_table('foo', 'id serial primary key, name varchar(128)')
        self.db.create_table('bar', 'name varchar(128), age int')
        self.db.reload()
        
        assert self.db.foo.primary_key == 'id'
        assert self.db.bar.primary_key == None
        
        x = self.db.foo.create()
        x.name = 'graham'
        x.save()
        
        assert x.get_primary() == 1
        assert type(x.get_primary()) == int or type(x.get_primary()) == long

    def test_safe_update(self):
        self.db.create_table('foo', 'id serial primary key, name varchar(128), age int')
        self.db.reload()

        x = self.db.foo.create()
        x.name = 'graham'
        x.age = 1
        x.save()
        
        d = {'id':100, 'name':'abbott', 'age':100}
        x.safe_update(__exclude__=['age'], **d)
        x.save()

        assert x.id == 1
        assert x.name == 'abbott'
        assert x.age == 1
        
    def test_update(self):
        self.db.create_table('foo', 'id serial primary key, name varchar(128), age int')
        self.db.reload()

        x = self.db.foo.create()
        x.name = 'graham'
        x.age = 1
        x.save()
        
        d = {'id':100, 'name':'abbott', 'age':100}
        x.safe_update(**d)
        x.save()

        assert x.id == 1
        assert x.name == 'abbott'
        assert x.age == 100
        
    def test_set_id(self):
        self.db.create_table('foo', 'id serial primary key, name varchar(128), age int')
        self.db.reload()

        x = self.db.foo.create()
        x.name = 'graham'
        x.age = 1
        x.save()

        x.id = 10
        x.save()
        
        assert x == self.db.foo.get(10)
        
    def test_delete_row(self):
        self.db.create_table('foo', 'id serial primary key, name varchar(128), age int')
        self.db.reload()

        x = self.db.foo.create()
        x.name = 'graham'
        x.age = 1
        x.save()

        assert x == self.db.foo.find_one()
        
        x.delete()
        
        assert (x not in self.db.foo.find()) == True
        
    def test_get_linked(self):
        self.db.create_table('foo', 'id serial primary key, name varchar(128), age int')
        self.db.create_table('bar', 'id serial primary key, bleh int, foo_row integer references foo(id)')
        self.db.create_table('baz', 'id serial primary key, foo_row integer references foo(id)')
        self.db.reload()

        x = self.db.foo.create()
        x.name = 'graham'
        x.age = 1
        x.save()        
    
        y = self.db.bar.create()
        y.bleh = 100
        y.foo_row = x
        y.save()
    
        z = self.db.baz.create()
        z.foo_row = x
        z.save()
        
        assert y in x.get_linked()
        assert z in x.get_linked()

    def test_unlink(self):
        self.db.create_table('foo', 'id serial primary key, name varchar(128), age int')
        self.db.create_table('bar', 'id serial primary key, bleh int, foo_row integer references foo(id)')
        self.db.reload()

        x = self.db.foo.create()
        x.name = 'graham'
        x.age = 1
        x.save()        
    
        y = self.db.bar.create()
        y.bleh = 100
        y.foo_row = x
        y.save()
        
        res = x.unlink()
        
        assert y in res
        assert y not in x.get_linked()
        
        y.reload()
        assert y.foo_row == None
        
    def test_relink(self):
        self.db.create_table('foo', 'id serial primary key, name varchar(128), age int')
        self.db.create_table('bar', 'id serial primary key, bleh int, foo_row integer references foo(id)')
        self.db.reload()

        x = self.db.foo.create()
        x.name = 'graham'
        x.age = 1
        x.save()        
    
        y = self.db.bar.create()
        y.bleh = 100
        y.foo_row = x
        y.save()
        
        z = self.db.foo.create()
        z.name = 'abbott'
        z.age = 2
        z.save()
        
        x.relink(z.id)
        
        assert x.get_linked() == []
        assert z.get_linked() == [y]
    
    def test_join(self):
        self.db.create_table('foo', 'id serial primary key, name varchar(128), age int')
        self.db.create_table('bar', 'id serial primary key, bleh int, foo_row integer references foo(id)')
        self.db.create_table('baz', 'id serial primary key, foo_row integer references foo(id)')
        self.db.reload()

        x = self.db.foo.create()
        x.name = 'graham'
        x.age = 1
        x.save()        
    
        y = self.db.bar.create()
        y.bleh = 100
        y.foo_row = x
        y.save()
    
        z = self.db.baz.create()
        z.foo_row = x
        z.save()
        
        assert y in x.join('bar')
        assert z not in x.join('bar')
        
        assert z in x.join('baz')
        assert y not in x.join('baz')
    
    def test_glue_with_pkey(self):
        self.db.create_table('foo', 'id serial primary key, name varchar(128)')
        self.db.create_table('grouper', 'id serial primary key, group_name varchar(128)')
        self.db.create_table('membership', 'id serial primary key, "foo" integer references foo(id), "group" integer references grouper(id)')
        self.db.reload()
        
        x = self.db.foo.create()
        x.name = 'graham'
        x.save()
        
        y = self.db.grouper.create()
        y.group_name = 'orm users'
        y.save()
        
        z = self.db.membership.create()
        z.foo = x
        z.group = y
        z.save()
        
        assert x.get_glue_table(self.db.membership) == self.db.grouper
        assert x.get_glue_table('membership') == self.db.grouper
        assert x.glue(self.db.membership) == [y]
        assert x.glue('membership') == [y]
        
        assert y.get_glue_table(self.db.membership) == self.db.foo
        assert y.get_glue_table('membership') == self.db.foo
        assert y.glue(self.db.membership) == [x]
        assert y.glue('membership') == [x]
    
        z = self.db.membership.create()
        z.foo = x
        z.group = y
        z.save()
    
    def test_glue_with_multiple_columns(self):
        self.db.create_table('foo', 'id serial primary key, name varchar(128)')
        self.db.create_table('grouper', 'id serial primary key, group_name varchar(128)')
        self.db.create_table('my_role', 'id serial primary key, name varchar(128)')
        self.db.create_table('membership', 'id serial primary key, \
                                           "foo" integer references foo(id), \
                                           "group_row" integer references grouper(id), \
                                           "role" integer references my_role(id)')
        
        self.db.reload()
        
        x = self.db.foo.create()
        x.name = 'graham'
        x.save()
        
        y = self.db.grouper.create()
        y.group_name = 'orm users'
        y.save()
        
        w = self.db.my_role.create()
        w.name = 'orm contributer'
        w.save()
        
        z = self.db.membership.create()
        z.foo = x
        z.group_row = y
        z.role = w
        z.save()
        
        assert x.get_glue_table(self.db.membership, 'group_row') == self.db.grouper
        assert x.get_glue_table('membership', 'group_row') == self.db.grouper
        assert x.glue(self.db.membership, 'group_row') == [y]
        assert x.glue('membership', 'group_row') == [y]
        
        assert y.get_glue_table(self.db.membership, 'foo') == self.db.foo
        assert y.get_glue_table('membership', 'foo') == self.db.foo
        assert y.glue(self.db.membership, 'foo') == [x]
        assert y.glue('membership', 'foo') == [x]
        
        z = self.db.membership.create()
        z.foo = x
        z.group = y
        z.save()
                
    def test_glue_without_pkey(self):
        self.db.create_table('foo', 'id serial primary key, name varchar(128)')
        self.db.create_table('grouper', 'id serial primary key, group_name varchar(128)')
        self.db.create_table('membership', '"foo" integer references foo(id), "group" integer references grouper(id)')
        self.db.reload()
        
        x = self.db.foo.create()
        x.name = 'graham'
        x.save()
        
        y = self.db.grouper.create()
        y.group_name = 'orm users'
        y.save()
        
        z = self.db.membership.create()
        z.foo = x
        z.group = y
        z.save()
        
        assert x.get_glue_table(self.db.membership) == self.db.grouper
        assert x.get_glue_table('membership') == self.db.grouper
        assert x.glue(self.db.membership) == [y]
        assert x.glue('membership') == [y]
        
        assert y.get_glue_table(self.db.membership) == self.db.foo
        assert y.get_glue_table('membership') == self.db.foo
        assert y.glue(self.db.membership) == [x]
        assert y.glue('membership') == [x]
    
        z = self.db.membership.create()
        z.foo = x
        z.group = y
        z.save()
    
    def test_duplicate(self):
        self.db.create_table('foo', 'id serial primary key, name varchar(128), age int')
        self.db.create_table('bar', 'id serial primary key, bleh int, foo_row integer references foo(id)')
        self.db.reload()

        x = self.db.foo.create()
        x.name = 'graham'
        x.age = 1
        x.save()        
    
        y = self.db.bar.create()
        y.bleh = 100
        y.foo_row = x
        y.save()

        z = x.duplicate()
        z.save()
        
        x.reload()
        z.reload()
        
        assert x.name == z.name
        assert x.age == z.age
        assert x.id != z.id