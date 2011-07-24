import os
import orm.unit_tests

orm.unit_tests.db_type = 'postgres'

for i in os.listdir(orm.unit_tests.__path__[0]):
    if i.startswith('test_') and i.endswith('.py'):
        print 'execing', i
        execfile(orm.unit_tests.__path__[0]+'/'+i)


print 'about to start'
import unittest
unittest.main()