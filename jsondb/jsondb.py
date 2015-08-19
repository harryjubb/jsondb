import collections
import math
import operator
import os
import re
import simplejson as json
import types

ops = {'>': operator.gt,
       '<': operator.lt,
       '>=': operator.ge,
       '<=': operator.le,
       '==': operator.eq}

def is_callable(obj):
    
    return hasattr(obj, '__call__')

def where(key, function, value, rev=False):
    
    if isinstance(function, types.StringTypes):
        function = ops[function]
    
    def comp_key_value(dictionary):
        
        if key not in dictionary:
            return False
        
        args = (dictionary[key], value)
        
        # SPECIFY REVERSED ORDER FOR SPECIFIC FUNCTIONS
        # OR USER CAN SPECIFY WITH THE `rev` KWARG
        if function is re.match or function is re.search or rev:
            args = tuple(reversed(args))
        
        return function(*args)
    
    return comp_key_value

def make_iterable(obj):
    
    if isinstance(obj, types.StringTypes) or not isinstance(obj, collections.Iterable):
        return (obj,)
    else:
        return obj

class JSONDBTable(object):
    '''
    
    A JSONDBTable is structured as a list of dictionaries:
    
    [
        {'id': 1,
         'key1': 'value1'},
         
        {'id'; 2,
         'key2': 'value2'}
    ]
    
    ---
    
    Nothing is written to disk until the sync() method is called.
    
    '''
    
    # CONTEXT HANDLING
    def __enter__(self):
        
        return self
    
    def __exit__(self, err_type, err_value, err_traceback):
        
        if err_type:
            print 'JSONDBTableError'
            print err_type
            print err_value
            print err_traceback
            raise err_type
        
        self.sync()
    
    # INITIALISATION
    def __init__(self, filename, indexes=[]):

        self.filename = filename        
        self._open(indexes=indexes)
            
    
    # FILESYSTEM INTERFACE
    def _open(self, indexes=[]):
        '''
        Load in JSON data from file if possible.
        '''

        if os.path.exists(self.filename):
            with open(self.filename, 'rb') as fo:
                self._data = json.load(fo)

            self._validate()
        
        else:
            
            self._data = []
        
        self._indexes = {}
        
        if indexes:
            for index in indexes:
                self.add_index(index)

    def sync(self):
        '''
        Write everything in the data structure
        to the JSON file on disk.
        '''
        
        # FIDELITY CHECK
        self._validate()

        # WRITE TO FILE
        # !!! OVERWRITES FILE CONTENTS
        with open(self.filename, 'wb') as fo:
            json.dump(self._data, fo)
            fo.flush()
            os.fsync(fo.fileno())
    
    # FIDELITY CHECKING
    def _validate(self):
        '''
        Ensure that the data fits the list-of-dicts paradigm.
        '''

        if not isinstance(self._data, list):
            raise ValueError('JSON DB table validation failed for data: {}'.format(self._data))

        if self._data:
            if any([not isinstance(x, dict) for x in self._data]):
                raise ValueError('JSON DB table validation failed for data: {}'.format(self._data))
    
    # INDEXES
    def add_index(self, key):
        '''
        '''
        
        self._indexes[key] = {} #dict((item[key], e) for e, item in enumerate(self._data))
        
        for e, item in enumerate(self._data):
            
            if item[key] in self._indexes:
                raise KeyError('Indexes must be unique.')
            
            else:
                self._indexes[key][item[key]] = e
    
    # CRUD
    
    # CREATE A DICTIONARY
    def add(self, item):
        '''
        '''
        
        if not isinstance(item, dict):
            raise ValueError('Only dict objects can be added to a JSON DB table.')
        
        self._data.append(item)
        
        # UPDATE INDEXES
        for key in self._indexes:
            self._indexes[key][item[key]] = len(self._data) - 1

    # READ ONE OR MORE DICTIONARIES
    def get(self, condition=None):
        '''
        '''
        
        if condition is None:
            return self._data
        
        else:
            
            if (isinstance(condition, tuple) or isinstance(condition, list)) \
               and len(condition) == 2:
                
                key_name = condition[0]
                key_value = condition[1]
                
                if key_name in self._indexes:
                    
                    try:
                        return [self._data[self._indexes[key_name][key_value]]]
                    except KeyError:
                        #print 'WARNING: Key not found ({})'.format(key_value)
                        return []
                    
                else:
                    raise IndexError('Index not found on {}'.format(key_name))
                
            else:
                return [x for x in self._data if condition(x)]
            
            return []
    
    def get_one(self, conditions=None):
        
        results = self.get(conditions)
        
        if results:
            return results[0]
    
    # UPDATE A DICTIONARY
    # CREATES IT IF THE CONDITION ISN'T FOUND
    def update(self, update_dict, condition=None, only_first=True):
        '''
        Creates if condition not matched in the JSONDBTable
        '''
        
        condition_matched = False
        
        if (isinstance(condition, tuple) or isinstance(condition, list)) \
           and len(condition) == 2:
            
            key_name = condition[0]
            key_value = condition[1]
            
            if key_name in self._indexes:
                if key_value in self._indexes[key_name]:
                    self._data[self._indexes[key_name][key_value]].update(update_dict)
                else:
                    # ADD IF NOT EXISTS
                    self.add(update_dict)
                    #raise IndexError('"{}" not in "{}" index'.format(key_value, key_name))
            else:
                raise IndexError('Index not found on {}'.format(key_name))
            
        else:
        
            for dictionary in self._data:
                
                if condition is None:
                    dictionary.update(update_dict)
                
                else:
                    
                    if condition(dictionary):
                        dictionary.update(update_dict)
                        condition_matched = True
                
                if condition_matched and only_first:
                    break
            
            if condition is None or not condition_matched:
                self.add(update_dict)
    
    # DELETE A DICTIONARY
    def delete_all(self):
        '''
        '''
        
        self._data = []
        
    # CONVENIENCE/SUGAR
    def append(self, item):
        
        self.add(item)
    
        
    # MAGIC METHODS
    def __repr__(self):
        return json.dumps(self._data)
    
    def __str__(self):
        return json.dumps(self._data, sort_keys=True, indent=2*' ')
    
    def __len__(self):
        return len(self._data)
    
    def __iter__(self):
        
        for item in self._data:
            yield item