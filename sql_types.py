import orm
import pickle
import datetime

class SQLType(object):
    py_type = None
    def __init__(self):
        self.dirty_on_touch = False

    def __str__(self):
        return self.__class__.__name__
    
    def post_load(self, data):
        return data

    def pre_save(self, data):
        if data == {} or data == [] or data == '' or data == None:
            return None
        else:
            if self.py_type:
                return self.py_type(data)
        return data

class SQLInteger(SQLType): 
    py_type = int
    def pre_save(self, data):
        if data == 'None':
            return None
        else:
            if type(data) == str:
                try:
                    return int(data)
                except:
                    return None
        return data
                
class SQLFloat(SQLType): py_type = float
class SQLDecimal(SQLType): py_type = float
class SQLVarChar(SQLType): py_type = str
class SQLPassword(SQLType): py_type = str
class SQLChar(SQLType): py_type = str
class SQLText(SQLType): py_type = str
class SQLDate(SQLType): py_type = str
class SQLDateTime(SQLType): py_type = str


class SQLBoolean(SQLType): 
    py_type = bool
    def pre_save(self, data):
        if type(data) == bool:
            return data
        else:
            if data == 'false' or data == 'False':
                return False
            elif data == 'true' or data == 'True':
                return True
            else:
                return bool(data)
                
class SQLTime(SQLType): py_type = str

class SQLPickle(SQLType):
    py_type = str
    def __init__(self):
        SQLType.__init__(self)
        self.dirty_on_touch = True

    def post_load(self, data):
        if data:
            return pickle.loads(str(data))
        else:
            return {}
        
    def pre_save(self, data):
        if data == None:
            data = {}
        return pickle.dumps(data)

class SQLBlob(SQLType): 
    py_type = str
    
    def pre_save(self, data):
        if data:
            return data.replace('\x00', '\\x00')
        else:
            return data
    
    def post_load(self, data):
        if data:
            return data.replace('\\x00', '\x00')
        else:
            return data

class SQLUnknown(SQLType):
    py_type = str
    def __init__(self, tname, name, num):
        print '*' * 40
        print '\t UNKNOWN TYPE: %s for column %s from %s' % (str(num), name, tname)
        print '*' * 40


loaded = 0

try:
    import Crypto.Cipher.AES
    class SQLEncrypt(SQLType):
        py_type = str
        def __init__(self, password='0123456789abcdef'):
            SQLType.__init__(self)
            self.aes_object = Crypto.Cipher.AES.new(password)
        
        def post_load(self, data):
            if data:
                tmp = self.aes_object.decrypt(data)
                tmp = tmp.rstrip(' ')[:-1]
                return tmp
            else:
                return None
        
        def pre_save(self, data):
            if data == None:
                return None
            else:
                data += '-'
                while len(data) % 16 != 0:
                    data += ' '
                
                return self.aes_object.encrypt(data)

except:

    import orm
    if orm.verbose_level() > 0:
        print 'cant use AES encryption for SQLEncrypted column type.'    

    class SQLEncrypt(SQLType):
        py_type = str
        def __init__(self, password='0123456789abcdef'):
            SQLType.__init__(self)
        
class SQLExternalFile(SQLType):
    py_type = str
    def __init__(self, file_prefix='/tmp/'):
        SQLType.__init__(self)
        self.file_prefix = file_prefix
        self.file_prefix = self.file_prefix.replace("~", '').replace('..', '')

    def post_load(self, data):            
        if type(data) == str:
            try:
                return open(self.file_prefix+data, 'r+')
            except:
                return open(self.file_prefix+data, 'w')
        
    def pre_save(self, data):
        if not data:
            data = orm.quick_hash()

        if type(data) == file:
            return data.name
        else:
            return data
            
types = {
          'serial':SQLInteger,
          'int':SQLInteger,
          'smallint':SQLInteger,
          'integer':SQLInteger, 
          'character varying':SQLVarChar, 
          'character': SQLChar,
          'numeric':SQLFloat,
          'real':SQLFloat,
          'float':SQLFloat,
          'decimal':SQLDecimal,
          'double precision':SQLFloat,
          'text':SQLText, 
          'time':SQLTime,
          'date':SQLDate,
          'datetime':SQLDateTime,
          'time without time zone':SQLTime,
          'timestamp':SQLDateTime, 
          'timestamp with time zone':SQLDateTime,
          'timestamp without time zone':SQLDateTime,
          'date':SQLDate,
          'boolean':SQLBoolean,
          'bool':SQLBoolean,
          'bytea':SQLBlob,
          'blob':SQLBlob
          }
          
constraints = [
    'UNIQUE',
    ]
