from datetime import datetime


queries = []
queryNames = []

def init(timePeriodLabel):
    global queries
    global queryNames
    current_date = datetime.now()
    current_date = current_date.strftime('%Y-%m-%d')

    # Create the correct time label format for the different time periods
    year_month_date = current_date.split('-')

    date_3_months = current_date
    new_year = int(year_month_date[0])
    new_month = int(year_month_date[1])-3
    new_day = int(year_month_date[2])
    if (new_month < 1):
        new_year = new_year - 1
        new_month = 12 - new_month
    date_3_months = str(new_year) + '-' + str(new_month).zfill(2) + '-' + str(new_day).zfill(2)

    date_6_months = current_date
    new_year = int(year_month_date[0])
    new_month = int(year_month_date[1])-6
    new_day = int(year_month_date[2])
    if (new_month < 1):
        new_year = new_year - 1
        new_month = 12 - new_month
    date_6_months = str(new_year) + '-' + str(new_month).zfill(2) + '-' + str(new_day).zfill(2)

    date_1_year = current_date
    new_year = int(year_month_date[0])-1
    new_month = int(year_month_date[1])
    new_day = int(year_month_date[2])
    date_1_year = str(new_year) + '-' + str(new_month).zfill(2) + '-' + str(new_day).zfill(2)

    date_ytd = current_date
    new_year = int(year_month_date[0])
    new_month = 1
    new_day = 1
    date_ytd = str(new_year) + '-' + str(new_month).zfill(2) + '-' + str(new_day).zfill(2)


    selectedDate = current_date
    if timePeriodLabel == '3_Months':
        selectedDate = date_3_months
    elif timePeriodLabel == '6_Months':
        selectedDate = date_6_months
    elif timePeriodLabel == '1_Year':
        selectedDate = date_1_year
    elif timePeriodLabel == 'YTD':
        selectedDate = date_ytd

    # ------------------------------------------------- #
    #             ADD QUERIES TO LIST HERE              #
    # ------------------------------------------------- #
    allTestsQuery = allTests(selectedDate)
    queries.append(allTestsQuery)
    queryNames.append("AllTests")


    avgRunTimeQuery = avgRunTime(selectedDate)
    queries.append(avgRunTimeQuery)
    queryNames.append("AvgRunTime")


    numberOfDaysUsedQuery = numberOfDaysUsed(selectedDate)
    queries.append(numberOfDaysUsedQuery)
    queryNames.append("NumberOfDaysUsed")


    timesUsedQuery = timesUsed(selectedDate)
    queries.append(timesUsedQuery)
    queryNames.append("TimesUsed")


    allSNsQuery = allSNs(selectedDate)
    queries.append(allSNsQuery)
    queryNames.append("AllSNs")

    # ----------------------------------------------------- #


# ----------------------------------------------------- #
#               DEFINE NEW QUERIES HERE                 #
# ----------------------------------------------------- # 
# Lists all tests in the specified time frame
def allTests(date):
    query = f"""SELECT * 
        FROM data 
        WHERE date >= '{date}' 
        ORDER BY name"""
    return query

# Lists the number of days - for utilization
def numberOfDaysUsed(date):
    query = f"""SELECT 
        name, COUNT(DISTINCT date)
        FROM data 
        WHERE date >= '{date}' 
        GROUP BY name"""
    return query

# Lists the average time taken for tests
def avgRunTime(date):
    # The case statement handles scenarios where test is conducted at midnight and the time rolls over
    query = f"""SELECT
        name, ROUND( SUM( case when (end_time-start_time)<0 then (1440-start_time + end_time) else (end_time-start_time) end ) / (COUNT(date)/1.0), 4)
        FROM data 
        WHERE date >= '{date}' 
        GROUP BY name"""
    return query

# Lists the times at which tests were executed
def timesUsed(date):
    query = f"""SELECT 
    name, date, start_time/60
    FROM data 
    WHERE date >= '{date}' 
    ORDER BY name"""
    return query

# Lists the number of tests ran for a SN tested in the specified time frame, 
# can identify first pass yield
def allSNs(date):
    query = f"""SELECT
        uut_name, uut_pn, uut_sn, COUNT(uut_sn)
        FROM data 
        WHERE date >= '{date}' AND uut_sn != '-' 
        GROUP BY uut_sn"""
    return query

# ----------------------------------------------------- #
