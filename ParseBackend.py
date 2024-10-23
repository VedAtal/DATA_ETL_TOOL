import os
import time
import xml.etree.ElementTree as ET
from multiprocessing import Pool
from DatabaseBackend import TestTag, HeaderTag


headerTags = [tags.name for tags in HeaderTag]
testTags = [tags.name for tags in TestTag]
fileCounter = 0
filesFailed = 0
mergeFileCounter = 0
directoryTimes = []
directoryFileCounter = []
directoryNewFileCounter = []


def get_immediate_subdirectories(networkDirectory):
    subdirectories = []
    for name in os.listdir(networkDirectory):
        path = os.path.join(networkDirectory, name)
        if os.path.isdir(path):
            subdirectories.append(name)
    return subdirectories


def parallel_get_values(networkDirectory, saveDirectory, merge=True):
    global fileCounter
    fileCounter = 0
    global filesFailed
    filesFailed = 0
    global mergeFileCounter
    mergeFileCounter = 0
    global directoryTimes
    global directoryFileCounter
    global directoryNewFileCounter
    
    start_time = time.time()
    if not os.path.exists(saveDirectory):
        os.mkdir(saveDirectory)

    # Get all xml files within current log directory
    files = []
    for fileName in os.listdir(networkDirectory):
        if fileName.endswith('xml'):
            if merge:
                if not os.path.exists(saveDirectory+"\\"+fileName):
                    files.append(os.path.join(networkDirectory, fileName))
                    mergeFileCounter = mergeFileCounter+1
                fileCounter = fileCounter+1
            else:
                files.append(os.path.join(networkDirectory, fileName))
                fileCounter = fileCounter+1

    # Create a argument group for each file so it can be used for multiprocessing
    arguments = [(file, saveDirectory) for file in files]

    # Starts processes for each file in current directory to be executed concurrently
    with Pool() as pool:
        fileResults = pool.map(worker, arguments)
        filesFailed = sum(fileResults)

    elapsedTime = time.time() - start_time
    directoryTimes.append(elapsedTime)
    directoryFileCounter.append(fileCounter)
    directoryNewFileCounter.append(mergeFileCounter-filesFailed)

    # Print time taken for each directory and confirmation for each directory
    if merge:
         print("Successfully parsed and stored " + str(mergeFileCounter-filesFailed) + " out of " + str(mergeFileCounter) + " files in " + 
            networkDirectory + " in: " + "{:.2f} seconds. ".format(elapsedTime) + str(fileCounter) + " total files with " +
            str(mergeFileCounter-filesFailed) + " new merged files.")
    else:
        print("Successfully parsed and stored " + str(fileCounter-filesFailed) + " out of " + str(fileCounter) + " files in " + 
            networkDirectory + " in: " + "{:.2f} seconds.".format(elapsedTime))
        
    print("Logs stored in " + saveDirectory + ".")
        
    print("Total time elapsed: " + "{:.2f} seconds.".format(sum(directoryTimes)))
    print("Total files in directories: " + str(sum(directoryFileCounter)) + ".")
    print("Total files added: " + str(sum(directoryNewFileCounter)) + ".")


def worker(args):
    fileName = None
    saveDirectory = None

    if args is not None:
        fileName = args[0]
        saveDirectory = args[1]

    # Get XML structure tree from current file
    tree = deserialize_xml(fileName)

    if tree is None:
        print("Tree cannot be parsed from xml document: " + fileName)
        return 1

    # Process tree to remove data we do not want
    processed_tree = prune_data(tree)
    if (processed_tree is not None):
        save_pruned_tree_local(fileName, saveDirectory, processed_tree)
    else:
        print("File cannot be pruned properly: " + fileName)
        return 1

    return 0


# Prune tree and parse data accordingly
def prune_data(elemTree):
    tests = elemTree.findall('test')
    if tests is None:
        print("File is missing the tests attribute.")
        return None

    # Remove test tags not in list
    for test in tests:
        for child in list(test):
            if child.tag not in testTags:
                test.remove(child)

    # Remove header tags not in list
    header = elemTree.find('header')
    if header is not None:
        for child in list(header):
            if child.tag not in headerTags:
                header.remove(child)
            elif child.tag == "start_date":
                currDate = child.text
                newDate = changeDateFormat(currDate)
                child.text = newDate
            elif child.tag == "start_time" or child.tag == "end_time":
                currTime = child.text
                newTime = changeTimeFormat(currTime)
                child.text = newTime
    else:
        print("File is missing the header attribute.")
        return None

    return elemTree


# Return xml strucutre tree
def deserialize_xml(fileName):
    try:
        tree = ET.parse(fileName)
        return tree
    except ET.ParseError:
        print(str(ET.ParseError) + " " + fileName)
        return None


# Save new processed XML file to save directory.
def save_pruned_tree_local(filename, saveDirectory, tree):
    filePath = saveDirectory + '\\' + os.path.basename(filename)
    tree.write(filePath, 'UTF-8')

    # Indicates whether a file has been stored in the database or not
    with open(f"{filePath}:{'database state'}", "w") as metadata:
       metadata.write('unstored')



# ANY TRANFORMATION METHODS TO CHANGE FIELD FORMATS
# Stores date in YYYY-MM-DD format
def changeDateFormat(currDate):
    try:
        splitDate = currDate.split(',')
        year = splitDate[2].strip()
        splitDate[1] = splitDate[1].strip()
        monthAndDay = splitDate[1].split(" ")
        month = monthAndDay[0]
        day = monthAndDay[1]
    except:
        return ""

    if month == "Jan":
        month = "01"
    elif month == "Feb":
        month = "02"
    elif month == "Mar":
        month = "03"
    elif month == "Apr":
        month = "04"
    elif month == "May":
        month = "05"
    elif month == "Jun":
        month = "06"
    elif month == "Jul":
        month = "07"
    elif month == "Aug":
        month = "08"
    elif month == "Sep":
        month = "09"
    elif month == "Oct":
        month = "10"
    elif month == "Nov":
        month = "11"
    elif month == "Dec":
        month = "12"

    newDate = year+"-"+month+"-"+day

    if (newDate is not None and len(newDate) == 9):
        newDate = newDate[:-1] + "0" + newDate[-1]

    return newDate


# Stores time in 0-1440 minute format
def changeTimeFormat(currTime):
    try:
        splitTime = currTime.split(" ")
        totalTime = 0

        hoursAndMinutes = splitTime[0].split(":")
        if int(hoursAndMinutes[0]) == 12:
            if splitTime[1] == "PM":
                totalTime += 720
                totalTime += int(hoursAndMinutes[1])
            else:
                totalTime += int(hoursAndMinutes[1])
        else:
            if splitTime[1] == "PM":
                totalTime += 720
            totalTime += int(hoursAndMinutes[0])*60
            totalTime += int(hoursAndMinutes[1])
    except:
        return ""

    return str(totalTime)

