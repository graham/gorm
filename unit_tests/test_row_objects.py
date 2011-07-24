import unittest
import time
import orm

from orm.unit_tests.my_test import Test_MyTest

class Test_TestRowObjects(Test_MyTest):
    def test_row_repr(self):
        self.db.create_table('person', 'id serial primary key, name varchar(128)')
        self.db.create_table('nickname', 'id serial primary key, nick varchar(128), person integer references person(id)')
        self.db.create_table('my_group', 'id serial primary key, name varchar(128)')
        self.db.create_table('membership', 'id serial primary key, "person" integer references person(id), "group" integer references my_group(id)')
        
        class MyRow(orm.row.SQLRow):
            table = 'person'
            join_alias = {'nicknames':'nickname'}
            glue_alias = {'groups':'membership'}
            
        self.db.reload()
        
        #Setup
        me = self.db.person.create(name='graham').save()
        nick_one = self.db.nickname.create(nick='grahamatron', person=me).save()
        nick_two = self.db.nickname.create(nick='graham-cracker', person=me).save()
        
        my_group = self.db.my_group.create(name='orm developers').save()
        self.db.my_group.create(name='jitsu developers').save()
        
        self.db.membership.create(person=me, group=my_group).save()
        
        assert me.nicknames == [nick_one, nick_two]
        assert me.groups == [my_group]
        
    def test_row_updated(self):
        self.db.create_table('person', 'id serial primary key, name varchar(128)')
        self.db.create_table('nickname', 'id serial primary key, nick varchar(128), person integer references person(id)')
        self.db.create_table('my_group', 'id serial primary key, name varchar(128)')
        self.db.create_table('membership', 'id serial primary key, "person" integer references person(id), "group" integer references my_group(id)')
        
        class MyRow(orm.row.SQLRow):
            table = 'person'
            join_alias = {'nicknames':'nickname'}
            glue_alias = {'groups':'membership'}
            
        self.db.reload()
        
        #Setup
        me = self.db.person.create(name='graham').save()
        nick_one = self.db.nickname.create(nick='grahamatron', person=me).save()
        nick_two = self.db.nickname.create(nick='graham-cracker', person=me).save()
        
        my_group = self.db.my_group.create(name='orm developers').save()
        my_group2 = self.db.my_group.create(name='jitsu developers').save()
        
        self.db.membership.create(person=me, group=my_group).save()
        
        assert me.nicknames == [nick_one, nick_two]
        assert me.groups == [my_group]
        
        self.db.membership.create(person=me, group=my_group2).save()
        
        assert me.groups == [my_group, my_group2]
