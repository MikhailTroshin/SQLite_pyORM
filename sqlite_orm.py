# -*- coding: utf-8 -*-
"""
Created on Wed Mar  4 14:08:29 2020

@author: TroshinMV

SQLite_simple_module
List of supported functions:
    - create connection () + 
    - extract current configuration + 
    - extract DB information to file (dict or json) + 
    
    - create table if not exist  +
    - modify table
    - drop table
    
    - insert new objects to table +
    - delete objects from table
    - update one value + 
    - update many values +
    - export to csv +
    
    - execute any other SQL query
"""

import sqlite3
from sqlite3 import Error
import time
import re
import json
import csv
import os


# Meta informations.
__version__ = '1.0.0'
__author__ = 'Mike Troshin'
__author_email__ = 'troshin-mikhail@inbox.ru'

################################################################################
# Class SQLiteDB (main)
################################################################################ 

class SQLiteDB:
    # Класс База данных
    # create connection  
    def __init__(self, path):
        """
        Parameters
        ----------
        path : TYPE
            DESCRIPTION.

        Returns connection (equal to database)
        -------
        TYPE
            DESCRIPTION.

        """
        self._connection = None
        self._table_names = None # список имен таблиц
        self._tables = {} # словарь, содержащий экземпляры классов Table c ключами - именами таблиц
        try:
            self._connection = sqlite3.connect(path)
            print('Connection successful')
            time.sleep(0.5)
        except Error as e:
            print('The error "%s" occurred' %e)            
        # update inner confiruration
        self._update_table_info()
        
        
################################################################################
# Basic API class functions
################################################################################

    # execute a query for our DB object
    def execute_query(self, query):
        cursor = self._connection.cursor()
        try:
            cursor.execute(query)
            self._connection.commit()
            print('Query executed successfully')
        except Error as e:
            print('The error "%s" occurred' %e)
    
    # execute a read query for our DB object
    def execute_read_query(self, query):
        cursor = self._connection.cursor()
        result = None
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except Error as e:
            print('The error "%s" occurred' %e)
            

################################################################################
# API class functions
################################################################################            
    
# _____________________ Table access _____________________    
    
    # return a _Table instance by name of a table         
    def table(self, table_name):
        # Возвращаем экземпляр класса Table
        if isinstance(self._tables[table_name], _Table):
            return self._tables[table_name]
        else:
            print("There are no table with such a name")
       
        
    # return a list of names of tables
    def tables(self):
        # возвращаем имена всех экземпляров класса Table
        return self._table_names


# ________________ DB structure exporting _______________         
            
    # export current db structure to dict
    def struct_to_dict(self):
        # return configuration of DB as dict of tables, that contains dict of columns with it's datatypes
        DB_dict = {}
        for table_name in self._tables: # dict of instances
            if isinstance(self._tables[table_name], _Table):
                DB_dict[table_name] = self._tables[table_name].column_pattern()
        return DB_dict
    
    
    # export current db structure to json (to json instance or to file)
    def struct_to_json(self, **kwargs):
        # return configuration of DB as a JSON
        file_path = None
        file_path = kwargs.get('output_file')
        DB_dict = {}
        for table_name in self._tables: # dict of instances
            if isinstance(self._tables[table_name], _Table):
                DB_dict[table_name] = self._tables[table_name].column_pattern()
        raw_path = r'{}'.format(file_path)
        # if file path exists - write JSON to file
        if file_path:
            with open(raw_path, 'w') as json_file:
                json.dump(DB_dict, json_file, indent=2)
        # if file path doesn't exist - return JSON
        else:
            return json.dumps(DB_dict)

            
# ________________ Basic table functions _________________  
      
    # create new table
    def create_table(self, table_name, column_pattern):
        if isinstance(self._tables[table_name], _Table):
            return self._tables[table_name]
        else:
            cursor = self._connection.cursor()    
            create_query = "CREATE TABLE IF NOT EXISTS " + str(table_name) + "( \n" + self._make_pattern(column_pattern) + ");"          
            try:
                cursor.execute(create_query)
                self._connection.commit()
                print(table_name + ' table executed successfully')
            except Error as e:
                print('The error "%s" occurred' %e)
            self._update_inner_info()
            
    
    # drop table - not ready
          
                 
    # modify table - not ready   
    
                   
################################################################################
# Inner functions
################################################################################
    
    # Make SQL request column config from dict column pattern      
    def _make_pattern(self, col_dict):
        # example of output: 'id SERIAL PRIMARY KEY, login CHAR(64), password CHAR(64)'
        # example of input: {'id':'SERIAL PRIMARY KEY', 'login':'CHAR(64)', 'password':'CHAR(64)'}
        __columns_desc = ' '
        __col_list = []
        try:
            for key, value in col_dict.items():
                __col_list.append(str(key) + ' ' + str(value))
            __columns_desc = ', \n'.join(__col_list)
            return __columns_desc
        except Exception:
            print('Incorrect input!')
        

    # Update configuration
    def _update_table_info(self):
        stop_words = ['FOREIGN']
        db_info_query = "SELECT name FROM sqlite_master WHERE type = 'table';"  
        cursor = self._connection.cursor()
        cursor.execute(db_info_query)
        table_tuples = cursor.fetchall()
        # [('users',), ('sqlite_sequence',), ('posts',), ('comments',), ('likes',)]
        self._table_names = [table_tuple[0] for table_tuple in table_tuples if table_tuple[0] != 'sqlite_sequence']
        if self._table_names:
            # if the database already exists and isn't empty 
            # for each table read the structure and create an Table class instance
            for table_name in self._table_names:
                table_structure_query = "SELECT sql FROM sqlite_master WHERE name = '" + table_name + "';"
                cursor = self._connection.cursor()
                cursor.execute(table_structure_query)
                table_struct_raw = cursor.fetchone()
                # ('CREATE TABLE users(\nid INTEGER PRIMARY KEY AUTOINCREMENT,\nname TEXT NOT NULL,\nage INTEGER,\ngender TEXT,\nnationality TEXT)',)
                list_of_columns = (re.sub('[\(\)]', '', re.search('\(.+\)', re.sub(r"\n", " ", table_struct_raw[0])).group(0))).split(',')
                columns = {} # future column pattern
                # for each column info parse the data and create the dict
                for column_info in list_of_columns:
                    column_name = re.findall(r'\w+', column_info)[0]
                    #self.column_names.append(column_name)
                    columns[column_name] = ' '.join(re.findall(r'\w+', column_info)[1:])
                # Delete objects from column dict, if a key is in stop words list
                for stop_word in stop_words:
                    if stop_word in columns.keys():
                        columns.pop(stop_word)
                self._tables[table_name] = _Table(self._connection,table_name,columns)
                #self._tables.append(_Table(self._connection,table_name,columns))
                time.sleep(0.5)
            print('Configuration is updated successfully')
        else:
            print('Database is empty')

      
################################################################################
# Class _Table (inner)
################################################################################        
          
class _Table:
    # class Table   
    def __init__(self, connection, table_name, column_pattern):
        self._connection = connection
        self._table_name = table_name
        self._column_pattern = {}
        self._column_pattern = column_pattern # dict
        self._columns = []
        for key in column_pattern.keys():    
            self._columns.append(key)

################################################################################
# API class functions
################################################################################             

# _____________________ Column access _____________________

    # return a list of names of columns in required table        
    def columns(self):
        return self._columns
    
    
    # return column pattern of a table as a dict
    def column_pattern(self):
        return self._column_pattern
    

# ___________________ Table exporting ____________________
    
    # export whole table to .csv file
    def to_csv(self, output_file):
        # return a table as a csv file
        query = "select * from '%s'" %self._table_name
        cursor = self._connection.cursor()  
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            
            with open(output_file, "w", newline='') as csv_file:
                csv_writer = csv.writer(csv_file, delimiter=",")
                csv_writer.writerow([i[0] for i in cursor.description])
                csv_writer.writerows(result)
            dirpath = os.getcwd() + "/" + output_file
            print ("Table data is exported successfully to {}".format(dirpath))
        except Error as e:
            print('The error "%s" occurred' %e)
            

# __________________ Basic  functions ____________________
  
    # insert a new record
    def insert(self, record):
        insert_query = "INSERT INTO %s VALUES " %self._table_name + _make_insert_values(record) + ";"
        _execute_query(self._connection, insert_query)


    # update one value
    def update(self, column, condition, value):
        update_query = "UPDATE %s SET "%self._table_name
                                        str(column) +
                                        " = " +
                                        str(value) +
                                        " WHERE " + str(condition))                  
        _execute_query(self._connection, update_query)
    
    
    # update many values
    def update_many(self, columns, condition, values):
        update_many_query = "UPDATE %s SET "%self._table_name + _make_many_insert_values(columns, condition, values)
        _execute_query(self._connection, update_many_query)
        

################################################################################
# Inner functions
################################################################################

    # Update column configuration - do not ready !!!!!!!!!!!!!!!!! 
    def _update_column_info(self):
        stop_words = ['FOREIGN']
        db_info_query = "SELECT name FROM sqlite_master WHERE type = 'table';"  
        cursor = self._connection.cursor()
        cursor.execute(db_info_query)
        table_tuples = cursor.fetchall()
        # [('users',), ('sqlite_sequence',), ('posts',), ('comments',), ('likes',)]
        self._table_names = [table_tuple[0] for table_tuple in table_tuples if table_tuple[0] != 'sqlite_sequence']
        if self._table_names:
            # if the database already exists and isn't empty 
            # for each table read the structure and create an Table class instance
            for table_name in self._table_names:
                table_structure_query = "SELECT sql FROM sqlite_master WHERE name = '" + table_name + "';"
                cursor = self._connection.cursor()
                cursor.execute(table_structure_query)
                table_struct_raw = cursor.fetchone()
                # ('CREATE TABLE users(\nid INTEGER PRIMARY KEY AUTOINCREMENT,\nname TEXT NOT NULL,\nage INTEGER,\ngender TEXT,\nnationality TEXT)',)
                list_of_columns = (re.sub('[\(\)]', '', re.search('\(.+\)', re.sub(r"\n", " ", table_struct_raw[0])).group(0))).split(',')
                columns = {} # future column pattern
                # for each column info parse the data and create the dict
                for column_info in list_of_columns:
                    column_name = re.findall(r'\w+', column_info)[0]
                    #self.column_names.append(column_name)
                    columns[column_name] = ' '.join(re.findall(r'\w+', column_info)[1:])
                # Delete objects from column dict, if a key is in stop words list
                for stop_word in stop_words:
                    if stop_word in columns.keys():
                        columns.pop(stop_word)
                self._tables[table_name] = _Table(self._connection,table_name,columns)
                #self._tables.append(_Table(self._connection,table_name,columns))
                time.sleep(0.5)
            print('Configuration is updated successfully')
        else:
            print('Database is empty')

################################################################################
# Static class functions
################################################################################

    # make a data part of insert query
    @staticmethod
    def _make_insert_values(record):
        result = []
        done = False
        if isinstance(record, (list, tuple)):
            for obj in record:
                done = True
                # check every inner object in the record to be a list or tuple
                if isinstance(obj, (list, tuple)):
                    result.append(tuple(obj))
                else:
                    result = []
                    done = False
                    break
            if not done:
                for obj in record:
                    done = True
                    # check every inner object in the record to be a value (int, float, string)
                    if isinstance(obj, (float, int, str)):
                        result.append(obj)
                    else:
                        done = False
                        break
            if done:
                return str(result)[1:-1]
            else:
                raise ValueError("Invalid input data")
        else:
            raise ValueError("Invalid input data")
    
    
    # make a data part of update_many query
    @staticmethod
    def _make_many_insert_values(columns, condition, values):
        result = ""
        if len(columns) == len(values):
            strings = []
            for column in columns:
                strings.append(str(column[i]) + " = " + str(values[i]))
            result = ", \n".join(strings) + "\nWHERE\n" + str(condition)
            return result
        else:
            raise ValueError("Different number of columns and values!")
            
        
    # execute a query
    @staticmethod
    def _execute_query(connection, query):
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            connection.commit()
            print('Query executed successfully')
        except Error as e:
            print('The error "%s" occurred' %e)
     
    
    # execute a read query
    @staticmethod
    def _execute_read_query(connection, query):
        cursor = connection.cursor()
        result = None
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except Error as e:
            print('The error "%s" occurred' %e)          


################################################################################
# Class _Column (inner)
################################################################################ 

class _Column:
    def __init__(self, connection, column_name, column_pattern):
        self._connection = connection
        self._column_name = column_name
        self._column_pattern = column_pattern
        


################################################################################
# Basic API function.
################################################################################
      
def create_connection(path):
    connection = None
    try:
        connection = sqlite3.connect(path)
        print('Connection successful')
    except Error as e:
        print('The error "%s" occurred' %e)   
    return connection

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print('Query executed successfully')
    except Error as e:
        print('The error "%s" occurred' %e)
 
def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print('The error "%s" occurred' %e)
  

################################################################################
# Testing
################################################################################


#________________ Testing ____________________      
test_list = [[1,2,'Fuck'], [3,4], [5,6]]
test_list_1 = [1, 2, 'Fuck']
test_tuple = ((1,2,'hello'), (3,4,'Hi'))
test_tuple_1 = (1,2,'hello')
print(_make_insert_values([(1, 'you'),(3, 'yo')]))
print(str(test_list))

_table_name = 'users'
def update(column, condition, value):
        update_query = ("UPDATE %s SET "%_table_name +
                                        str(column) +
                                        " = " +
                                        str(value) +
                                        " WHERE " + str(condition))                 
        #_execute_query(self._connection, update_query)
        return update_query

print(update('user', "post.id=5", 5))

strings = ['column_1 = new_value_1', 'column_2 = new_value_2']
", \n".join(strings)



        
columns = {'id': 'PINTEGER PRIMARY KEY AUTOINCREMENT', 
           'name': 'TEXT NOT NULL', 
           'age': 'INTEGER'}
columns_desc = ' '
col_list = []
for key, value in columns.items():
    col_list.append(str(key) + ' ' + str(value))
columns_desc = ', \n'.join(col_list)

conn = sqlite3.connect("G:\\sm_app.sqlite")#("C:\\Users\troshinmv\Python_Scripts\SQL\sm_app.sqlite")

db_info_query = "SELECT name, data_type FROM sqlite_master WHERE type = 'table';"
table_structure_query = "SELECT sql FROM sqlite_master WHERE name = 'users';"

table_structure_query = "SELECT sql FROM sqlite_master WHERE name = 'posts';"

db_tables = ['users', 'posts', 'comments', 'likes']
# по каждой таблице создать схему (имена столбцов)
for table in db_tables:
    table_structure_query = "SELECT sql FROM sqlite_master WHERE name = '" + table + "';"
    cursor = conn.cursor()
    cursor.execute(table_structure_query)
    table_struct_raw = cursor.fetchone()
    # ('CREATE TABLE users(\nid INTEGER PRIMARY KEY AUTOINCREMENT,\nname TEXT NOT NULL,\nage INTEGER,\ngender TEXT,\nnationality TEXT)',)
    result = (re.sub('[\(\)]', '', re.search('\(.+\)', re.sub(r"\n", " ", table_struct_raw[0])).group(0))).split(',')
    column_names = []
    columns = {}
    for column_info in result:
        column_names.append(re.findall(r'\w+', column_info)[0])
        columns[re.findall(r'\w+', column_info)[0]] = ' '.join(re.findall(r'\w+', column_info)[1:])
    

# чтение имен столбцов и типов их данных
res = ('CREATE TABLE users(\nid INTEGER PRIMARY KEY AUTOINCREMENT,\nname TEXT NOT NULL,\nage INTEGER,\ngender TEXT,\nnationality TEXT)',)
result = (re.sub('[\(\)]', '', re.search('\(.+\)', re.sub(r"\n", " ", res[0])).group(0))).split(',')
column_names = []
columns = {}
for column_info in result:
    column_names.append(re.findall(r'\w+', column_info)[0])
    columns[re.findall(r'\w+', column_info)[0]] = ' '.join(re.findall(r'\w+', column_info)[1:])
   

db_info_query = "SELECT name FROM sqlite_master WHERE type = 'table';"  
table_structure_query = "SELECT sql FROM sqlite_master WHERE name = 'users';"    

class DB:
    def __init__(self, connection, table_name_list, columns_list):
        self.connection = connection
        self.tables = []
        i = 0
        for table_name in table_name_list:
            self.table_name = Table(columns_list[i])
            self.tables.append(table_name)
            i += 1
        
    def create_table(self, table_name, columns):
        self.table_name = Table(columns)
        self.tables.append(table_name)
        
    def table(self, table_name):
        return self.table_name
    
    def all_tables(self):
        return self.tables
    
class Table:
    def __init__(self, columns):
        self._columns = columns
        
    def columns(self):
        return self._columns


my_db = DB('new_connection', ['my_table1', 'my_table2'], ['First', 'Second'])

my_db.create_table('my_table1', ['First', 'Second'])
my_db.table('my_table1').columns()

isinstance('my_table2', Table)

my_db.create_table('my_table2', ['Fuck', 'You'])
my_db.table('my_table2').columns
my_db.all_tables()


test_DB = SQLiteDB("G:\\sm_app.sqlite")
test_DB.tables()
test_DB.table('likes').columns()
print(test_DB.to_dict())
test_DB.to_json(output_file="Test_DB.json")
test_DB.tables()

# write to csv test
cursor = conn.cursor()
cursor.execute("select * from 'users'")
with open("users_data.csv", "w", newline='') as csv_file:
    csv_writer = csv.writer(csv_file, delimiter=",")
    csv_writer.writerow([i[0] for i in cursor.description])
    csv_writer.writerows(cursor.fetchall())

dirpath = os.getcwd() + "/users_data.csv"
print ("Data exported Successfully into {}".format(dirpath))



#Test classes
class MyClass: 
    def __init__(self, name): 
        self.name = name 
        self.checkme = 'awesome {}'.format(self.name) ... 
        instanceNames = ['red', 'green', 'blue'] # Here you use the dictionary holder = {name: MyClass(name=name) for name in instanceNames} 

holder['red'].checkme 
 
objs = list() for i in range(10): objs.append(MyClass()) 

        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        