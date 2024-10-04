# Banks Project ETL
Top 10 largest banks in the world ranked by market capitalization in billion USD. Further, the data needs to be transformed and stored in GBP, EUR and INR as well, in accordance with the exchange rate information that has been made available to you as a CSV file. The processed information table is to be saved locally in a CSV format and as a database table.

Your job is to create an automated system to generate this information so that the same can be executed in every financial quarter to prepare the report.

Particulars of the code to be made have been shared below.

<table>
  <thead>
    <tr>
      <th>Parameter</th>
      <th>Value</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>Code name</td>
      <td>
        <code>banks_project.py</code>
      </td>
    </tr>
     <tr>
      <td>Data URL</td>
      <td>
        <code>https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks</code>
      </td>
    </tr>
     <tr>
      <td>Exchange rate CSV path</td>
      <td>
        <a href="https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv">https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv</a>
      </td>
    </tr>
     <tr>
      <td>Table Attributes (upon Extraction only)</td>
      <td>
        <code>Name</code>,
        <code>MC_USD_Billion</code>
      </td>
    </tr>
     <tr>
      <td>Table Attributes (final)</td>
      <td>
        <code>Name</code>,
        <code>MC_USD_Billion</code>,
        <code>MC_GBP_Billion</code>,
        <code>MC_EUR_Billion</code>,
        <code>MC_INR_Billion</code>
      </td>
    </tr>
     <tr>
      <td>Output CSV Path</td>
      <td>
        <code>./Largest_banks_data.csv</code>
      </td>
    </tr>
     <tr>
      <td>Database name</td>
      <td>
        <code>Banks.db</code>
      </td>
    </tr>
     <tr>
      <td>Table name</td>
      <td>
        <code>Largest_banks</code>
      </td>
    </tr>
     <tr>
      <td>Log file</td>
      <td>
        <code>code_log.txt</code>
      </td>
    </tr>
  </tbody>
</table>

## Task 1 : Logging function 
Write the function to log the progress of the code, ```log_progress()```. 

The function accepts the message to be logged and enters it to a text file ```code_log.txt```.

The format to be used for logging must have the syntax:
```
<time_stamp> : <message>
```
You must associate the correct log entries with each of the executed function calls. Use the following table to note the logging message at the end of each function call that follows.

<table>
  <thead>
    <tr>
      <td>Task</td>
      <td>Log message on completion</td>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>Declaring known values</td>
      <td>Preliminaries complete. Initiating ETL process</td>
    </tr>
    <tr>
      <td>Declaring known values</td>
      <td>Preliminaries complete. Initiating ETL process</td>
    </tr>
    <tr>
      <td>Call extract() function</td>
      <td>Data extraction complete. Initiating Transformation process</td>
    </tr>
    <tr>
      <td>Call transform() function</td>
      <td>Data transformation complete. Initiating Loading process</td>
    </tr>
    <tr>
      <td>Call load_to_csv()</td>
      <td>Data saved to CSV file</td>
    </tr>
    <tr>
      <td>Initiate SQLite3 connection</td>
      <td>SQL Connection initiated</td>
    </tr>
    <tr>
      <td>Call load_to_db()</td>
      <td>Data loaded to Database as a table, Executing queries</td>
    </tr>
    <tr>
      <td>Call run_query()</td>
      <td>Process Complete</td>
    </tr>
     <tr>
      <td>Close SQLite3 connection</td>
      <td>Server Connection closed</td>
    </tr>
  </tbody>
</table>

### Task 1 : Answer
```Python
def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Month-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')
```

## Task 2 : Extraction of data
Analyze the webpage on the given URL:
```
https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks
```
Identify the position of the required table under the heading ```By market capitalization```. Write the function ```extract()```  to retrieve the information of the table to a Pandas data frame.

Note: Remember to remove the last character from the ```Market Cap```  column contents, like, '\n', and typecast the value to float format.

Write a function call for ```extract()```  and print the returning data frame.

![Zrzut ekranu 2024-10-03 195101](https://github.com/user-attachments/assets/e3bfbd76-1047-4301-82db-ffbe72822ad2)

### Task 2 : Answer
```Python
def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    
    df = pd.DataFrame(columns=table_attribs)
    
    tables = soup.find_all("tbody")
    rows = tables[0].find_all("tr")
    
    for row in rows:
        if row.find("td") is not None:
            col = row.find_all("td")
            bank_name = col[1].find_all('a')[1]['title']
            market_cap = col[2].contents[0][:-1]
            data_dict = {"Name": bank_name,
                         "MC_USD_Billion": float(market_cap)}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)

    return df
```

## Task 3 : Transformation of data
The Transform function needs to perform the following tasks:

  1. Read the exchange rate CSV file and convert the contents to a dictionary so that the contents of the first columns are the keys to the dictionary and the contents of the second column are the corresponding values.

  2. Add 3 different columns to the dataframe, viz. ```MC_GBP_Billion```, ```MC_EUR_Billion``` and ```MC_INR_Billion```, each containing the content of ```MC_USD_Billion```  scaled by the corresponding exchange rate factor. Remember to round the resulting data to 2 decimal places.

### Task 3 : Answer
```Python
def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    
    dataframe = pd.read_csv(csv_path)
    exchange_rate = dataframe.set_index('Currency').to_dict()['Rate']
    
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]
    
    return df
```

## Task 4: Loading to CSV
Write the function to load the transformed data frame to a CSV file, like ```load_to_csv()```, in the path mentioned in the project scenario.

### Task 4 : Answer
```Python
def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

    df.to_csv(output_path)
```

## Task 5: Loading to Database
Write the function to load the transformed data frame to an SQL database, like, ```load_to_db()```. Use the database and table names as mentioned in the project scenario.

* Before calling this function, initiate the connection to the SQLite3 database server with the name ```Banks.db```. Pass this connection object, along with the required table name ```Largest_banks```  and the transformed data frame, to the ```load_to_db()```  function in the function call.

Upon successful function call, you will have loaded the contents of the table with the required data and the file ```Banks.db```  will be visible in the Explorer tab of the IDE under the project folder.

### Task 5 : Answer
```Python
def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

    df.to_sql(table_name, sql_connection, if_exists = 'replace', index = False)
```

## Task 6: Function to Run queries on Database
Write the function ```run_queries()```  that accepts the query statement, and the SQLite3 Connection object, and generates the output of the query. The query statement should be printed along with the query output.

Execute 3 function calls using the queries as mentioned below.

  1. Print the contents of the entire table

Query statement:
```
SELECT * FROM Largest_banks
```

  2. Print the average market capitalization of all the banks in Billion USD.
     
Query statement:
```
SELECT AVG(MC_GBP_Billion) FROM Largest_banks
```

  3. Print only the names of the top 5 banks
     
Query statement:
```
SELECT Name from Largest_banks LIMIT 5
```

### Task 6 : Answer
```Python
def run_query(query_statement, sql_connection):

    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
```

## Task 7: Verify log entries
After updating all the ```log_progress()```  function calls, you have to run the code for a final execution. 

* However, you will first have to remove the ```code_log.txt file```, that would have been created and updated throughout the multiple executions of the code in this lab. You may remove the file using the following command on a terminal.
```
rm code_log.txt
```
Once the existing file is removed, now run the final execution. Upon successful completion of execution, open the ```code_log.txt```  file by clicking on it in the Explorer tab of the toolbar on left side of the programming pane of the IDE, under the project folder. You should see all the relevant entries made in the text file in relation to the stages of code execution.

```Python
''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']
db_name = 'Banks.db'
sql_connection = sqlite3.connect(db_name)
table_name = 'Largest_banks'
csv_path = 'exchange_rate.csv'
output_path = "./Largest_banks_data.csv"
log_file = "code_log.txt"
```
```Python
log_progress("Preliminaries complete. Initiating ETL process")

df = extract(url, table_attribs)
print(df)

log_progress("Data extraction complete. Initiating Transformation process")

df = transform(df, csv_path)
print(df)

log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(df, output_path)

log_progress("Data saved to CSV file")

sql_connection = sqlite3.connect(db_name)

log_progress("SQL Connection initiated")

load_to_db(df, sql_connection, table_name)

log_progress("Data loaded to Database as a table, Executing queries")

query_statement = f"SELECT * from {table_name}"
run_query(query_statement, sql_connection)

query_statement = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_query(query_statement, sql_connection)

query_statement = f"SELECT Name from {table_name} LIMIT 5"
run_query(query_statement, sql_connection)

log_progress("Process Complete")

sql_connection.close()

log_progress("Server Connection closed")
```
```Python
with open(log_file, "r") as log:
    LogContent = log.read()
    print(LogContent)
```
