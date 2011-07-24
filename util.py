import random
import time
import os

def quick_ts():
    return int(time.time())
    
def quick_hash(id=0):
    import sha
    tmp = sha.new()
    tmp.update( str(id) + str(random.randint(1000, 100000)) + time.asctime() + ' '.join(os.uname()) + os.getlogin() + str(os.getpid()) + str(os.getloadavg()))
    return tmp.hexdigest()

def unique_list(l):
    pk = []
    result = []
    for i in l:
        if i.get_primary() in pk:
            pass
        else:
            pk.append(i.get_primary())
            result.append(i)
            
    return result
    
def sort_by(rows, field):
    def sorter(left, right):
        l = getattr(left, field, None)
        r = getattr(right, field, None)

        if l < r:
            return -1
        elif l == r:
            return 0
        else: # l > r
            return 1

    rows.sort(cmp=sorter)
    return rows
    
def get_all_subclasses(klass):
    all = []
    first = klass.__subclasses__()
    cur = first
    new_cur = first
    
    while new_cur:
        new_cur = []
        for i in cur:
            if i not in all:
                all.append(i)
            
            for j in i.__subclasses__():
                if j not in all:
                    all.append(j)

                new_cur += j.__subclasses__()
        cur = new_cur
    return all
    
class SQLNone(object):
    pass

