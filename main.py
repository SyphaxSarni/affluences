import pandas as pd
import datetime as dt


ACTUAL_TIME = dt.datetime(2021, 5, 6, 15, 0, 0)

data_table = pd.read_csv("timetables.csv")
data_time = pd.read_csv("data.csv")

def is_open(site):
    data = data_table[data_table.site_id == site]
    if data.isnull().values.any():
        return False
    else:
        for j in data['opening_datetime'].astype('datetime64[ns]'):
            if ACTUAL_TIME >= j:
                for s in data['closing_datetime'].astype('datetime64[ns]'):
                    if ACTUAL_TIME <= s:
                        return True
                    else:
                        return False

for i in data_time.values:
    if(is_open(i[2])):
        if(dt.datetime.strptime(i[3],"%Y-%m-%d %H:%M:%S") > ACTUAL_TIME + dt(days=2)):
            print("Sensor {sensor name} with identifier {identifier} triggers an\ "
                  "alert at {alert datetime} with level {alert level} with last "
                  "data recorded at {last record datetime}")
        elif(dt.datetime.strptime(i[3],"%Y-%m-%d %H:%M:%S") > ACTUAL_TIME + dt(days=1)):
            print("Sensor {sensor name} with identifier {identifier} triggers an\ "
                  "alert at {alert datetime} with level {alert level} with last "
                  "data recorded at {last record datetime}")
        elif(dt.datetime.strptime(i[3],"%Y-%m-%d %H:%M:%S") > ACTUAL_TIME + dt(hours=2)):
            print("Sensor {sensor name} with identifier {identifier} triggers an\ "
                  "alert at {alert datetime} with level {alert level} with last "
                  "data recorded at {last record datetime}")


