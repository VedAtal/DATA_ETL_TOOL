from multiprocessing import Pool
from datetime import datetime
import os
import sqlite3
import time
from enum import Enum
import xml.etree.ElementTree as ET
import Queries
import csv


# Lists what Test field tags should be kept
class TestTag(Enum):
    test_type = 0
    stim_pin = 1
    meas_pin = 2
    fail_text = 3
    timestamp = 4
    stim_volt = 5
    meas_value = 6
    ITR_Line_Number = 7


# Lists what Header field tags should be kept
class HeaderTag(Enum):
    UUT_name = 0
    UUT_pn = 1
    UUT_sn = 2
    ITR_file = 3
    ITR_fileCRC = 4
    start_date = 5
    start_time = 6
    end_date = 7
    end_time = 8
    test_status = 9
    elapsed_time = 10
    DMM_Self_Test = 11
    DMM_Cal_Date = 12
    date = 13


# Used to create database table and schemas
def initialize(databaseName):
    conn = connect(databaseName)
    curs = conn.cursor()

    # ----------------------------------------------------- #
    #     TO ADD MORE TABLES TO THE DATABASE DO IT HERE     #
    # ----------------------------------------------------- #
    curs.execute('''CREATE TABLE IF NOT EXISTS data (
        name VARCHAR(64),
        uut_name VARCHAR(64),
        uut_pn VARCHAR(64),
        uut_sn VARCHAR(64),
        date DATE,
        start_time INTEGER,
        end_time INTEGER)
    ''')

    # ----------------------------------------------------- #

    commit_and_close(conn)


def get_immediate_subdirectories(localDirectory):
    subdirectories = []
    for name in os.listdir(localDirectory):
        path = os.path.join(localDirectory, name)
        if os.path.isdir(path):
            subdirectories.append(name)
    return subdirectories


# ----------------------------------------------------- #
#      TO ADD MORE DATA TO THE DATABASE DO IT HERE      #
# ----------------------------------------------------- #
insert_data_query = '''INSERT INTO data VALUES(?,?,?,?,?,?,?)'''

def store_data(tree, databaseName, folderName):
    conn = connect(databaseName)
    curs = conn.cursor()
    header = tree.find('header')

    # CPATS Data table
    name = folderName
    uut_name = None
    uut_pn = None
    uut_sn = None
    date = None
    start_time = None
    end_time = None

    if header is not None:
        for child in list(header):
            if child.tag == "UUT_name":
                uut_name = child.text
            elif child.tag == "UUT_pn":
                uut_pn = child.text
            elif child.tag == "UUT_sn":
                uut_sn = child.text
            elif child.tag == "start_date":
                date = child.text
            elif child.tag == "start_time":
                try:
                    start_time = int(child.text)
                except:
                    start_time = 'NULL'
            elif child.tag == "end_time":
                try:
                    end_time = int(child.text)
                except:
                    end_time = 'NULL'
    try:
        # Uses the insert query to insert a new query for this database table
        curs.execute(insert_data_query, (name, uut_name, uut_pn, uut_sn, date, start_time, end_time))
        return 0
    except sqlite3.Error:
        print(str(sqlite3.Error) + " " + folderName)
        return 1
    
# ----------------------------------------------------- #


def execute_sql_queries(databaseName, queriesSavePath, timePeriod):
    current_date = datetime.now()
    current_date = current_date.strftime('%Y-%m-%d')

    timePeriodLabel = ""
    if timePeriod == '1':
        timePeriodLabel = "3_Months"
    elif timePeriod == '2':
        timePeriodLabel = "6_Months"
    elif timePeriod == '3':
        timePeriodLabel = "1_Year"
    elif timePeriod == '4':
        timePeriodLabel = "YTD"

    Queries.init(timePeriodLabel)
        
    conn = connect(databaseName)
    curs = conn.cursor()

    # Go through all SQL queries to execute and save results from
    counter = 0
    for query in Queries.queries:
        curs.execute(query)
        results = curs.fetchall()
       
        fileName = current_date + "_" + Queries.queryNames[counter] + "_" + timePeriodLabel + '.csv'
        with open(queriesSavePath + '\\' + fileName, 'w') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(results)
        
        print('SQL query #' + str(counter) + '\'s results stored to ' + fileName)

        counter+=1
        
    commit_and_close(conn)


# Method to wipe the database's current tables
def wipe_database(databaseName):
    conn = connect(databaseName)
    curs = conn.cursor()
    curs.execute('''DELETE FROM data''')


fileCounter = 0
filesFailed = 0
newFileCounter = 0
directoryTimes = []
directoryFileCounter = []
directoryNewFileCounter = []

def parallel_get_values(localDirectory, databaseName, folderName, refresh=False):
    global fileCounter
    fileCounter = 0
    global filesFailed
    filesFailed = 0
    global newFileCounter
    newFileCounter = 0
    global directoryTimes
    global directoryFileCounter
    global directoryNewFileCounter

    start_time = time.time()
    # Get all xml files within current log directory
    files = []
    for fileName in os.listdir(localDirectory):
        if fileName.endswith('xml'):
            filePath = localDirectory + '\\' + fileName
            # Indicates whether a file has been stored in the database or not
            fileStored = ""
            with open(f"{filePath}:{'database state'}", "r") as metadata:
                fileStored = metadata.read()

            if fileStored == "unstored" or refresh:
                files.append(os.path.join(localDirectory, fileName))
                newFileCounter = newFileCounter+1
                with open(f"{filePath}:{'database state'}", "w") as metadata:
                    metadata.write('stored')
           
            fileCounter = fileCounter+1

    # Create a argument group for each file so it can be used for multiprocessing
    arguments = [(file, databaseName, folderName) for file in files]

    # Starts processes for each file in current directory to be executed concurrently
    with Pool() as pool:
        fileResults = pool.map(worker, arguments)
        filesFailed = sum(fileResults)

    elapsedTime = time.time() - start_time
    directoryTimes.append(elapsedTime)
    directoryFileCounter.append(fileCounter)
    directoryNewFileCounter.append(newFileCounter-filesFailed)

    # Print time taken for each directory and confirmation for each directory
    print("Successfully stored " + str(newFileCounter-filesFailed) + " out of " + str(newFileCounter) + " files to the database in: " 
          + "{:.2f} seconds. ".format(elapsedTime) + str(fileCounter) + " total files in directory with " + 
          str(newFileCounter-filesFailed) + " new files.")
        
    print("Total time elapsed: " + "{:.2f} seconds.".format(sum(directoryTimes)))
    print("Total files in directories: " + str(sum(directoryFileCounter)) + ".")
    print("Total files added: " + str(sum(directoryNewFileCounter)) + ".")


def worker(args):
    fileName = None
    databaseName = None
    folderName = None

    if args is not None:
        fileName = args[0]
        databaseName = args[1]
        folderName = args[2]

    # Get XML structure tree from current file
    tree = deserialize_xml(fileName)

    if tree is None:
        print("Tree cannot be parsed from xml document: " + fileName)
        return 1
    
    if store_data(tree, databaseName, folderName) == 1:
        print("Cannot store data from xml document: " + fileName)
        return 1
    
    return 0


# Return xml strucutre tree
def deserialize_xml(fileName):
    try:
        tree = ET.parse(fileName)
        return tree
    except ET.ParseError:
        print(str(ET.ParseError) + " " + fileName)
        return None


# Connect to the database
def connect(databaseName):
    conn = sqlite3.connect(databaseName, isolation_level=None, timeout=5)
    conn.execute('PRAGMA cache_size = 10000')
    conn.execute('PRAGMA journal_mode = WAL')
    conn.execute('PRAGMA synchronous = OFF')
    return conn


# Commit the changes and close the connection
def commit_and_close(conn):
    conn.commit()
    conn.close()
