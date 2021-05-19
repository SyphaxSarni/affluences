import pandas as pd


def sensor_names(cnx):
    if cnx is not None:
        query = "SELECT sensor_name, sensor_id FROM sensors"
        dataframe = pd.read_sql(query, con=cnx)
        return dataframe
    else:
        return None


def open_sensors(cnx, datetime):
    if cnx is not None:
        date = datetime.date()
        time = datetime.time()
        query = (
                "SELECT sensor_id FROM "
                "sensors_settings s INNER JOIN timetables t ON s.site_id = t.site_id WHERE '" + str(date) +
                "' BETWEEN opening_day AND closing_day AND '" + str(time) + "' BETWEEN opening_time AND closing_time")
        dataframe = pd.read_sql(query, con=cnx)
        return dataframe
    else:
        return None


def alerts_bounds(cnx, datetime, maxdate, mindate=None):
    date = datetime.date()
    time = datetime.time()
    if mindate is None and maxdate is not None:
        subquery = "< '"+ str(maxdate)+"'"
    elif mindate is not None and maxdate is not None:
        subquery = "BETWEEN '"+str(mindate)+"' AND '"+str(maxdate)+"'"
    else:
        print("Error : min date can be none but not max date")
        return None
    if cnx is not None:
        query = ("WITH CTE AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY sensor_id ORDER BY record_datetime DESC) AS "
                 "rn FROM records where record_datetime <= '" + str(date) + " " + str(time) + "') SELECT sensor_id, record_datetime FROM CTE WHERE "
                 "rn = 1 AND record_datetime "+subquery)
        dataframe = pd.read_sql(query, con=cnx)
        return dataframe
    else:
        return None

