import os
import orm.unit_tests

for i in os.listdir(orm.unit_tests.__path__[0]):
    if i.startswith('test_') and i.endswith('.py'):
        execfile(orm.unit_tests.__path__[0]+'/'+i)
        
import unittest
unittest.main()
