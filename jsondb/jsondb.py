import collections
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
        
        try:
            args = (dictionary[key], value)
        except KeyError:
            return False
        
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
    def __init__(self, filename):

        self.filename = filename        
        self._open()
    
    # FILESYSTEM INTERFACE
    def _open(self):
        '''
        Load in JSON data from file if possible.
        '''

        if os.path.exists(self.filename):
            with open(self.filename, 'rb') as fo:
                self._data = json.load(fo)

            self._validate()
        
        else:
            
            self._data = []

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

    # CRUD
    
    # CREATE A DICTIONARY
    def add(self, item):
        '''
        '''
        
        if not isinstance(item, dict):
            raise ValueError('Only dict objects can be added to a JSON DB table.')
        
        self._data.append(item)

    # READ ONE OR MORE DICTIONARIES
    def get(self, conditions=None):
        '''
        '''
        
        if conditions is None:
            return self._data
        
        else:
            
            conditions = make_iterable(conditions)
            
            return [x for x in self._data if any(condition(x) for condition in conditions)]
    
    def get_one(self, conditions=None):
        
        results = self.get(conditions)
        
        if results:
            return results[0]
    
    # UPDATE A DICTIONARY
    # CREATES IT IF THE CONDITION ISN'T FOUND
    def update(self, update_dict, condition=None):
        '''
        Creates if condition not matched in the JSONDBTable
        '''
        
        condition_found = False
        
        if condition is None:
            condition_found = True
        
        for dictionary in self._data:
            
            if condition is None:
                dictionary.update(update_dict)
            
            else:
                
                if condition(dictionary):
                    dictionary.update(update_dict)
                    condition_found = True
        
        if not condition_found:
            self.add(update_dict)
    
    # DELETE A DICTIONARY
    def delete(self, condition=None):
        '''
        '''
        
        if condition is None:
            self._data = []
        
        #else:
        
    # CONVENIENCE/SUGAR
        
    # MAGIC METHODS
    def __repr__(self):
        return json.dumps(self._data)
    
    def __str__(self):
        return json.dumps(self._data, sort_keys=True, indent=2*' ')
    
    def __len__(self):
        return len(self._data)