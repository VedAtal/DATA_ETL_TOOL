*************************************************************
Data ETL Tool
Ved Atal - 2024
*************************************************************

Designed to pull XML logs from specified network (or local) directory.
Can be slightly tweaked to work for different formats such as JSON, CSV, etc.
In general the overarching strucutre stays the same but some changes need to be made:
 * Specified network or local directory to pull data from
 * Change format of saving data based on need
 * Identify what fields/tags are being extracted and transformed in log files
 * Configure Database schema and query schema
 * Existing code is an example of all of these

Descripton of the different options:
1. Pull data from network drive and save (dist\semiprocessed).
 * This one will collect files from the network drive and parse them to their new formatting and then store them locally in the semiprocessed directory.
 * Only files that have not previously been parsed and stored here are added when this option is run, so that we are not adding all files everytime, this would take too long.
    * 1.1. There is a no-merge option if it is needed to re-add all files in directory which is listed under option 1.1.
    * Useful if there might have been corruption with the semiprocessed folder, or a new format for the files is being used, so it is necessary to re-add them all.

2. Insert data (dist\semiprocessed) into database (dist\database\Logs.db).
 * This one will collect all the parsed files from the local semiprocessed drive and store them on a local SQLite database, Logs.db.
 * Only files that have not previously been added will be added here to save time and also if we re-add files we would have duplicate data.
    * 2.1. There is a refresh option if it is needed to re-add all files in the local directory to the database. This option wipes the database and readds everything.
    * Useful if there might have been corruption with the Logs.db file, or a new format for the data being stored, so it is necessary to re-add them all.

3. Execute SQL queries on database and save results to (dist\queries).
 * This one will run the SQL queries in Queries.py and store the results from all the queries in the queries local folder.
 * The general format for the results are "DateOfExecution_QueryType_TimePeriod.csv".
 * This option has the selection of running the queries on 4 different time periods.
    * Last 3 Months
    * Last 6 months
    * Last year (last 12 months)
    * YTD (year-to-date)

4. Print last data upload date and last data fetch date.
 * Simply prints the last time data was fetched from network drive and last time data was uploaded to database.
 * Useful to keep track of what data is currently parsed and when a new fetch and upload should be performed.

5. Exit program.
 * As the title entails, this is to exit the program. 
 * You could also exit by entering anything that is not an option here.



Technical Details:
**************************************************************
main.py:
Handles initial bootup of program and selection for which function needs to be executed.

ParseBackend.py:
First step of the logging process. Performs an ETL (Extract, Transform, Load), which is a fancy way of saying it parses the files to the specified format we want, 
and then it will store them on the local data warehouse (the local drive), procedure on network files and stores them all in the 'dist\semiprocessed' local directory.

DatabaseBackend.py:
Second step of the logging proces. Loads all files in the 'dist\semiprocessed' directory to the local SQLite database at 'dist\database\Logs.db', where data can be queried.
Also part of the third step, which is running the SQL queries on the database. This performs the predefined queries and stores the results to 'dist\queries'.

Queries.py:
New queries can be added here to gain new data. 

LastDataFetch.txt and LastDataUpload.txt:
These just store the last dates for each of these actions, as mentioned in option 4.

Some python dependancies to know:
 * Multiprocessing (pool)
 * XML Element Tree
 * SQLite 3
 * OS

*************************************************************
