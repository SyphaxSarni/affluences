import pandas as pd
import datetime as dt
from datetime import datetime


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
        if(ACTUAL_TIME > datetime.strptime(i[3],"%Y-%m-%d %H:%M:%S") + dt.timedelta(days=2)):
            print("Sensor "+ i[1]+" with identifier" + str(i[0])+" triggers an "
                  "alert at "+str(ACTUAL_TIME)+" with level 3 with last "
                  "data recorded at "+i[3])
        elif( ACTUAL_TIME > datetime.strptime(i[3],"%Y-%m-%d %H:%M:%S") + dt.timedelta(days=1)):
            print("Sensor " + i[1] + " with identifier" + str(i[0]) + " triggers an "
                  "alert at " + str(ACTUAL_TIME) + " with level 2 with last "
                  "data recorded at " + i[3])
        elif(ACTUAL_TIME > datetime.strptime(i[3],"%Y-%m-%d %H:%M:%S") + dt.timedelta(hours=2)):
            print("Sensor " + i[1] + " with identifier" + str(i[0]) + " triggers an "
                  "alert at " + str(ACTUAL_TIME) + " with level 1 with last "
                  "data recorded at " + i[3])


