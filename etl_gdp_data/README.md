# Extract, Transfrom and Load GDP Data

## Project Scenario:
An international firm that is looking to expand its business in different countries across the world has recruited you. You have been hired as a junior Data Engineer and are tasked with creating an automated script that can extract the list of all countries in order of their GDPs in billion USDs (rounded to 2 decimal places), as logged by the International Monetary Fund (IMF). Since IMF releases this evaluation twice a year, this code will be used by the organization to extract the information as it is updated.

The required data seems to be available on the URL mentioned below:

URL
```
'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
```

The required information needs to be made accessible as a ```CSV```  file ```Countries_by_GDP.csv```  as well as a table ```Countries_by_GDP```  in a database file ```World_Economies.db```  with attributes ```Country```  and ```GDP_USD_billion```.

Your boss wants you to demonstrate the success of this code by running a query on the database table to display only the entries with more than a 100 billion USD economy. Also, you should log in a file with the entire process of execution named ```etl_project_log.txt```.

You must create a Python code 'etl_project_gdp.py' that performs all the required tasks.

## Objectives
You have to complete the following tasks for this project

    1. Write a data extraction function to retrieve the relevant information from the required URL.

    2. Transform the available GDP information into 'Billion USD' from 'Million USD'.

    3. Load the transformed information to the required CSV file and as a database file.

    4. Run the required query on the database.

    5. Log the progress of the code with appropriate timestamps.

## Task 1: Extracting information
Extraction of information from a web page is done using the web scraping process. For this, you'll have to analyze the link and come up with the strategy of how to get the required information. The following points are worth observing for this task.

Inspect the URL and note the position of the table. Note that even the images with captions in them are stored in tabular format. Hence, in the given webpage, our table is at the third position, or index 2. Among this, we require the entries under 'Country/Territory' and 'IMF -> Estimate'.

Note that there are a few entries in which the IMF estimate is shown to be '—'. Also, there is an entry at the top named 'World', which we do not require. Segregate this entry from the others because this entry does not have a hyperlink and all others in the table do. So you can take advantage of that and access only the rows for which the entry under 'Country/Terriroty' has a hyperlink associated with it.

Note that '—' is a special character and not a general hyphen, '-'. Copy the character from the instructions here to use in the code.

Assuming the function gets the URL and the table_attribs parameters as arguments, complete the function extract() in the code following the steps below.

1. Extract the web page as text.

    * Use the 'requests.get()' function with 'text' attribute.

2. Parse the text into an HTML object.

    * Use the 'BeautifulSoup()' function with the 'html.parser' argument.

3. Create an empty pandas DataFrame named df with columns as the table_attribs.

    * Use the 'pandas.DataFrame' function with the 'column' argument set as table_attribs.

4. Extract all 'tbody' attributes of the HTML object and then extract all the rows of the index 2 table using the 'tr' attribute.

    * Use the 'find_all()' function of the HTML object to gather all attributes of specific type.

5. Check the contents of each row, having attribute ‘td’, for the following conditions.

    a. The row should not be empty.
    b. The first column should contain a hyperlink.
    c. The third column should not be '—'.

    * Run a for loop and check the conditions using if statements.

6. Store all entries matching the conditions in step 5 to a dictionary with keys the same as entries of table_attribs. Append all these dictionaries one by one to the dataframe.

    * You'll need the pandas.concat() function to append the dictionary. Remember to keep the ignore_index parameter as 'True'.

```Python
def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {"Country": col[0].a.contents[0],
                             "GDP_USD_millions": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
    return df
```

## Task 2: Transform information
The transform function needs to modify the ‘GDP_USD_millions’. You need to cover the following points as a part of the transformation process.

1. Convert the contents of the 'GDP_USD_millions' column of df dataframe from currency format to floating numbers.

    a. Save the dataframe column as a list. b. Iterate over the contents of the list and use split() and join() functions to convert the currency text into numerical text. Type cast the numerical text to float.

2. Divide all these values by 1000 and round it to 2 decimal places.

    * Use the numpy.round() function for rounding. Assign the modified list back to the dataframe.

3. Modify the name of the column from 'GDP_USD_millions' to 'GDP_USD_billions'.

    * You'll need the df.rename() function.

```Python
def transform(df):
    GDP_list = df["GDP_USD_millions"].tolist()
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]
    GDP_list = [np.round(x/1000,2) for x in GDP_list]
    df["GDP_USD_millions"] = GDP_list
    df=df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"})
    return df
```
## Task 3: Loading information
Loading process for this project is two fold.

1. You have to save the transformed dataframe to a CSV file. For this, pass the dataframe df and the CSV file path to the function load_to_csv() and add the required statements there.

    * Use the 'to_csv()' function object for the pandas dataframe.

```Python
def load_to_csv(df, csv_path):
    df.to_csv(csv_path)
```

2. You have to save the transformed dataframe as a table in the database. This needs to be implemented in the function load_to_db(), which accepts the dataframe df, the connection object to the SQL database conn, and the table name variable table_name to be used.

    * Use the 'to_sql()' function object for the pandas dataframe.

```Python
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
```

## Task 4: Querying the database table
Assuming that the appropriate query was initiated and the query statement has been passed to the function run_query(), along with the SQL connection object sql_connection and the table name variable table_name, this function should run the query statement on the table and retrieve the output as a filtered dataframe. This dataframe can then be simply printed.

    * Use the pandas.read_sql() function to run the query on the database table.

```Python
def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
```

## Task 5: Logging progress
Logging needs to be done using the log_progress() funciton. This function will be called multiple times throughout the execution of this code and will be asked to add a log entry in a .txt file, etl_project_log.txt. The entry is supposed to be in the following format:

```
<Time_stamp> : <message_text>
```

Here, message text is passed to the function as an argument. Each entry must be in a separate line.

    * Use datetime.now() function to get the current timestamp.

```Python
def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')
```

## Function calls
```Python
log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('World_Economies.db')

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()
```