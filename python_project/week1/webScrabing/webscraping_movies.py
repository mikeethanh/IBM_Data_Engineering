import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

#Initialization of known entities

#You must declare a few entities at the beginning. For example, you know the required URL, the CSV name for saving the record, the database name, 
#and the table name for storing the record. You also know the entities to be saved. Additionally, since you require only the top 50 results, you will require a loop counter initialized to 0. You may initialize all these by using the following code in webscraping_movies.py:

url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = 'Movies.db'
table_name = 'Top_50'
csv_path = 'top_50_films.csv'
df = pd.DataFrame(columns=["Average Rank","Film","Year"])
count = 0

#Loading the webpage for Webscraping

#To access the required information from the web page, you first need to load the entire web page as an HTML document in python using the requests.get().text function and then parse the text in the HTML format using BeautifulSoup to enable extraction of relevant information.

#Đầu tiên, nó sử dụng thư viện requests
#Phương thức .get(url) của requests được sử dụng để gửi yêu cầu GET đến URL. .text được sử dụng để trả về nội dung của phản hồi dưới dạng văn bản.
#thư viện BeautifulSoup để phân tích nội dung HTML của trang web.
html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')

#Scraping of required information

#You now need to write the loop to extract the appropriate information from the web page. The rows of the table needed can be accessed using the find_all() function with the BeautifulSoup object using the statements below.

#Phương thức find_all của đối tượng BeautifulSoup (data) được sử dụng để tìm tất cả các thẻ <tbody> trong nội dung HTML đã phân tích.
tables = data.find_all('tbody')
# Sau khi tìm được tất cả các thẻ <tbody>, đoạn mã này tiếp tục tìm tất cả các dòng <tr> trong bảng đầu tiên (tables[0]). 
rows = tables[0].find_all('tr')

#You can now iterate over the rows to find the required data. Use the code shown below to extract the information.

for row in rows:
    if count<50:
        col = row.find_all('td')
        if len(col)!=0:
            data_dict = {"Average Rank": col[0].contents[0],
                         "Film": col[1].contents[0],
                         "Year": col[2].contents[0]}
            #Khi bạn cung cấp index=[0], Pandas sẽ tạo một DataFrame với một dòng duy nhất có index là 0. Điều này hữu ích khi bạn muốn tạo một DataFrame 
            #từ một từ điển (dictionary) với các giá trị đã biết trước và chỉ muốn có một dòng duy nhất trong DataFrame đó.
            df1 = pd.DataFrame(data_dict, index=[0])
            #Tham số ignore_index=True trong hàm pd.concat() được sử dụng để bỏ qua các index hiện có của các DataFrame được nối và tạo một index mới, tuần tự từ 0 đến tổng số hàng của DataFrame mới.
            df = pd.concat([df,df1], ignore_index=True)
            count+=1
    else:
        break

#The code functions as follows.

print(df)


#Storing the data
#After the dataframe has been created, you can save it to a CSV file using the following command:

df.to_csv(csv_path)

#Remember that you defined the variable csv_path earlier.

#To store the required data in a database, you first need to initialize a connection to the database, save the dataframe as a table, and then close the connection. This can be done using the following code:

conn = sqlite3.connect(db_name)
df.to_sql(table_name, conn, if_exists='replace', index=False)
conn.close()