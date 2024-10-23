from datetime import datetime
from multiprocessing import freeze_support
import ParseBackend
import DatabaseBackend


# Where the netowork log files are stored
networkDrivePath = r'pathtodrive'
# Where to save the processed files
savePath = r'dist\semiprocessed'
# Where to save the database
databasePath = r'dist\database\Logs.db'
# Where to save the queries
queriesSavePath = r'dist\queries'

def menu():
    # Merge option for data warehouse (semiprocessed)
    merge = True
    # Refresh option for database (logs.db)
    refresh = False

    global savePath
    global networkDrivePath
    print("Welcome to the menu!")
    print("1. Pull data from network drive (" + networkDrivePath + ") and save (" + savePath + ").")
    print("  1.1. No-merge option.")
    print("2. Insert data (" + savePath + ") into database (" + databasePath + ").")
    print("  2.1. Refresh option.")
    print("3. Execute SQL queries on database and save results to (" + queriesSavePath + ").")
    print("4. Print last data upload date and last data fetch date.")
    print("5. Exit program.")
    choice = input("Enter your choice: ")

    print()

    if choice == "1" or choice == "1.1":
        # Confirmation for running no-merge
        if (choice == "1.1"):
            print("Are you sure you want to run a 'no-merge'? This will re-add all files from network directory (~45 minutes) and also wipe the database.")
            choice = input("Enter your choice [Y/n]: ")
            print()
            if (choice == 'Y'):
                merge = False
                DatabaseBackend.wipe_database(databasePath)
            elif (choice == 'n'):
                print("Aborting 'no-merge' option.")
                print()
                return
            else:
                print("Invalid choice, enter 'Y' or 'n'.")
                print()
                return
        
        print("Grabbing xml tags...")

        # Get all log directories in the overarching log folder
        directories = ParseBackend.get_immediate_subdirectories(networkDrivePath)

        # Run parallels to parse files in each of those log directories
        count = 0
        size = str(len(directories))
        for directory in directories:
            count+=1
            print("Starting parallels in: " + directory + "  ---  " + str(count) + "/" + size)
            ParseBackend.parallel_get_values(networkDrivePath + "\\" + directory, savePath + "\\" + directory[5:-3], merge)
            print()
        
        # Update the fetch date
        current_date = datetime.now()
        current_date = current_date.strftime('%Y-%m-%d')
        file = open("LastDataFetch.txt","w") 
        file.write(current_date) 
        file.close()

    elif choice == "2" or choice == "2.1":
        DatabaseBackend.initialize(databasePath)
        # Confirmation for the refresh option
        if (choice == "2.1"):
            print("Are you sure you want to run a 'refresh'? This will wipe and re-add all files to database (~10 minutes if all files are pulled from network drive).")
            choice = input("Enter your choice [Y/n]: ")
            print()
            if (choice == 'Y'):
                refresh = True
                DatabaseBackend.wipe_database(databasePath)
            elif (choice == 'n'):
                print("Aborting 'refresh' option.")
                print()
                return
            else:
                print("Invalid choice, enter 'Y' or 'n'.")
                print()
                return
        
        print("Grabbing xml tags...")

        # Get all directories in the parsed and saved folder
        directories = DatabaseBackend.get_immediate_subdirectories(savePath)

        # Each directory represents logs for a given
        count = 0
        size = str(len(directories))
        for directory in directories:
            count+=1
            print("Starting parallels in: " + directory + "  ---  " + str(count) + "/" + size)
            DatabaseBackend.parallel_get_values(savePath + "\\" + directory, databasePath, directory, refresh)
            print()
        
        # Update the upload date
        current_date = datetime.now()
        current_date = current_date.strftime('%Y-%m-%d')
        file = open("LastDataUpload.txt","w") 
        file.write(current_date) 
        file.close()

    elif choice == "3":
        # Choose the time period to run the queries on
        print("What time period do you want to run the queries on?")
        print("1. 3 Months")
        print("2. 6 Months")
        print("3. 1 Year")
        print("4. YTD")
        timePeriod = input("Enter your choice: ")

        print()

        if timePeriod != '1' and timePeriod != '2' and timePeriod != '3' and timePeriod != '4':
            print("Invalid choice.")
            print()
            return

        print("Executing SQL queries...")

        DatabaseBackend.execute_sql_queries(databasePath, queriesSavePath, timePeriod)
       
    elif choice == "4":
        file = open("LastDataFetch.txt","r") 
        print("Last data fetch date: " + file.readline())
        file.close()

        file = open("LastDataUpload.txt","r") 
        print("Last data upload date: " + file.readline())
        file.close()

    elif choice == "5":
        print("Exiting program.")

    else:
        print("Invalid choice.")

    print()


if __name__ == '__main__':
    freeze_support() # Not too important, just used for some init for multiprocessing.
    menu()
