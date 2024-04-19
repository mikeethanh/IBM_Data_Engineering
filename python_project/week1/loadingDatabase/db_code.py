#Database initiation

import sqlite3
import pandas as pd
#Now, you can use SQLite3 to create and connect your process to a new database STAFF using the following statements.
conn = sqlite3.connect('STAFF.db')

#Create and Load the table

#To create a table in the database, you first need to have the attributes of the required table. Attributes are columns of the table. Along with their names, the knowledge of their data types are also required. The attributes for the required tables in this lab were shared in the Lab Scenario.
table_name = 'INSTRUCTOR'
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']

#Reading the CSV file

#Now, to read the CSV using Pandas, you use the read_csv() function. Since this CSV does not contain headers, you can use the keys of the attribute_dict dictionary as a list to assign headers to the data. For this, add the commands below to db_code.py

file_path = 'INSTRUCTOR.csv'
df = pd.read_csv(file_path, names = attribute_list)

#Loading the data to a table

#if_exists='replace': Tham số này xác định hành động sẽ thực hiện nếu bảng có cùng tên đã tồn tại trong cơ sở dữ liệu. Trong trường hợp này, 'replace' sẽ ghi đè lên bảng có sẵn nếu nó tồn tại, tức là nó sẽ xóa bảng cũ và tạo bảng mới với dữ liệu từ DataFrame.
#index=False: Tham số này xác định xem liệu chỉ số của DataFrame (df) có được bao gồm trong bảng SQL hay không. Khi thiết lập index=False, chỉ số của DataFrame sẽ không được sử dụng để tạo cột trong bảng SQL. Điều này hữu ích khi bạn không muốn bao gồm chỉ số của DataFrame trong cơ sở dữ liệu
df.to_sql(table_name, conn, if_exists = 'replace', index =False)
print('Table is ready')

#1.Viewing all the data in the table.
#Add the following lines of code to db_code.py

#sử dụng f-string
query_statement = f"SELECT * FROM {table_name}"
#query_output là kết quả của câu lệnh SQL được thực thi, được lưu trữ dưới dạng một đối tượng DataFrame. Cụ thể, đoạn mã sử dụng pd.read_sql() để thực thi câu lệnh SQL và đọc kết quả vào một DataFrame.
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

#2.Viewing only FNAME column of data.
query_statement = f"SELECT FNAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

#3.Viewing the total number of entries in the table.
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

#Now try appending some data to the table. Consider the following.
#a. Assume the ID is 100.
#b. Assume the first name, FNAME, is John.
#c. Assume the last name as LNAME, Doe.
#d. Assume the city of residence, CITY is Paris.
#e. Assume the country code, CCODE is FR.

data_dict = {'ID' : [100],
            'FNAME' : ['John'],
            'LNAME' : ['Doe'],
            'CITY' : ['Paris'],
            'CCODE' : ['FR']}
data_append = pd.DataFrame(data_dict)

#Now use the following statement to append the data to the INSTRUCTOR table.
data_append.to_sql(table_name, conn, if_exists = 'append', index =False)
print('Data appended successfully')

conn.close()
