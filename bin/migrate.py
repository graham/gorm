#!/usr/bin/env python2.5

from jitsu.core.portal import *

import orm.migrate
db = jitsu.get_db()

if len(sys.argv) < 2:
    prefix = raw_input('What is the sql prefix you want to store: ')
else:
    prefix = sys.argv[1]
if len(sys.argv) < 3:
    location = raw_input('What is the location of the sql files: ')
else:
    location = sys.argv[2]

orm.migrate.easy_migrate(db, prefix, location)
