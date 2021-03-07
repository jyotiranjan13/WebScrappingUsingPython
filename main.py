

# Import necessary packages
from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
import psycopg2
import sql
import os.path

# from sqlalchemy import create_engine
# from sqlalchemy import column,String
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.orm import sessionmaker

conn = psycopg2.connect(
    host="localhost",
    database="fastapi",
    user="postgres",
    password="ranjan@1979")
#
# connection.autocommit = False
# cur = connection.cursor()

def checkTableExists(connection, tablename):
    dbcur = connection.cursor()
    dbcur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if dbcur.fetchone()[0] == 1:
        dbcur.close()
        return True

    dbcur.close()
    return False




# Site URL
url="https://agmarknet.gov.in/SearchCmmMkt.aspx?Tx_Commodity=24&Tx_State=0&Tx_District=0&Tx_Market=0&DateFrom=01-Jan-2020&DateTo=31-Dec-2020&Fr_Date=01-Jan-2020&To_Date=31-Dec-2020&Tx_Trend=0&Tx_CommodityHead=Potato&Tx_StateHead=--Select--&Tx_DistrictHead=--Select--&Tx_MarketHead=--Select--"

# Make a GET request to fetch the raw HTML content
html_content = requests.get(url).text

# Parse HTML code for the entire site
soup = BeautifulSoup(html_content, "lxml")
#print(soup.prettify()) # print the parsed data of html

gdp = soup.find_all("table", attrs={"class": "tableagmark_new"})
print("Number of tables on site: ",len(gdp))

# Lets go ahead and scrape first table with HTML code gdp[0]
table1 = gdp[0]
# the head will form our column names
body = table1.find_all("tr")
# Head values (Column names) are the first items of the body list
head = body[0] # 0th item is the header row
body_rows = body[1:] # All other items becomes the rest of the rows

# Lets now iterate through the head HTML code and make list of clean headings

# Declare empty list to keep Columns names
headings = []
for item in head.find_all("th"): # loop through all th elements
    # convert the th elements to text and strip "\n"
    item = (item.text).rstrip("\n")
    #print("".join(item.split()) )
    # append the clean column name to headings
    headings.append("".join(item.split()))
print(headings)
# Next is now to loop though the rest of the rows

#print(body_rows[0])
all_rows = [] # will be a list for list for all rows
for row_num in range(len(body_rows)): # A row at a time
    row = [] # this will old entries for one row
    for row_item in body_rows[row_num].find_all("td"): #loop through all row entries
        # row_item.text removes the tags from the entries
        # the following regex is to remove \xa0 and \n and comma from row_item.text
        # xa0 encodes the flag, \n is the newline and comma separates thousands in numbers
        aa = re.sub("(\xa0)|(\n)|,","",row_item.text)
        # aa=(row_item.text.st
        # print(aa)
        #append aa to row - note one row entry is being appended
        row.append(aa)
        # print(row)
    # append one row to all_rows
    all_rows.append(row)
    # print(all_rows)
    df = pd.DataFrame(data=all_rows, columns=headings)
    df.to_excel('PriceList.xlsx', index=True)
    from sqlalchemy import create_engine
    engine = create_engine('postgresql://postgres:ranjan@1979@localhost:5432/fastapi')
    table_name = 'pricelist1'
    con = engine.connect()
    # print(engine.table_names())
    df.to_sql(table_name, engine, if_exists = 'replace', chunksize = None)










