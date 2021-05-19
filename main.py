import mysql.connector as my
import datetime as dt
from dateutil.parser import parse
from mysql_utils import *

CURRENT_TIME = dt.datetime(2021, 5, 5, 15, 00, 00)
cnx = None


def menu():
    print("======================= Python/SQL Technique Test =======================")
    print("\t CURRENT TIME : " + str(CURRENT_TIME) + "\n")
    print("\t 1. Change current time")
    print("\t 2. Alerts level 1")
    print("\t 3. Alerts level 2")
    print("\t 4. Alerts level 3")
    print("\t 5. Alerts all levels")
    print("\t 6. Quit\n")


def to_date(string):
    try:
        date = parse(string, fuzzy=False)
        return date
    except ValueError:
        return None


def print_alerts(data, level=1):
    if data is None or data.empty:
        print("\nNo alerts of level "+str(level))
    else:
        print("\nAlerts:")
        for i in data.values.tolist():
             print("Sensor " + i[0] + " with identifier " + str(i[1]) + " triggers an alert at " +
                    str(CURRENT_TIME) + " with level "+str(level)+" with last data recorded at "+str(i[2]))


def main():
    global cnx
    config = {
        'user': 'root',
        'password': 'root123',
        'host': '127.0.0.1',
        'database': 'test_technique',
        'raise_on_warnings': True
    }
    print("Connection to database..")
    try:
        cnx = my.connect(**config)
    except my.Error as err:
        if err.errno == my.errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your username and password")
        elif err.errno == my.errorcode.ER_BAD_DB_ERROR:
            print("Database doesn't exist")
        else:
            print(err)

    if cnx is None:
        print("Connection Failed")
        exit()
    else:
        print("Connection succeed")
        sensors_names = sensor_names(cnx)
        while True:
            menu()
            choice = int(input("Your choice : "))
            if choice == 1:
                global CURRENT_TIME
                value = str(input("New current time in a valid format :"))
                new_date = to_date(value)
                if new_date is not None:
                    CURRENT_TIME = new_date
                    print("New current time changed")
                else:
                    print("Error : bad date format try (YYYY-MM-DD HH:mm:SS)")
            elif choice == 2:
                alert1 = alerts_bounds(cnx,CURRENT_TIME,CURRENT_TIME - dt.timedelta(hours=2), CURRENT_TIME - dt.timedelta(days=1))
                open_sensor = open_sensors(cnx,CURRENT_TIME)
                alert1_dataframe = pd.merge(pd.merge(sensors_names, open_sensor, on="sensor_id"), alert1, on="sensor_id")
                print_alerts(alert1_dataframe,1)
            elif choice == 3:
                alert2 = alerts_bounds(cnx, CURRENT_TIME, CURRENT_TIME - dt.timedelta(days=1), CURRENT_TIME - dt.timedelta(days=2))
                open_sensor = open_sensors(cnx, CURRENT_TIME)
                alert2_dataframe = pd.merge(pd.merge(sensors_names, open_sensor, on="sensor_id"), alert2, on="sensor_id")
                print_alerts(alert2_dataframe, 2)
            elif choice == 4:
                alert3 = alerts_bounds(cnx, CURRENT_TIME, CURRENT_TIME - dt.timedelta(days=2))
                open_sensor = open_sensors(cnx, CURRENT_TIME)
                alert3_dataframe = pd.merge(pd.merge(sensors_names, open_sensor, on="sensor_id"), alert3, on="sensor_id")
                print_alerts(alert3_dataframe, 3)
            elif choice == 5:
                alert1 = alerts_bounds(cnx, CURRENT_TIME,CURRENT_TIME - dt.timedelta(hours=2), CURRENT_TIME - dt.timedelta(days=1))
                alert2 = alerts_bounds(cnx, CURRENT_TIME, CURRENT_TIME - dt.timedelta(days=1), CURRENT_TIME - dt.timedelta(days=2))
                alert3 = alerts_bounds(cnx, CURRENT_TIME, CURRENT_TIME - dt.timedelta(days=2))
                open_sensor = open_sensors(cnx, CURRENT_TIME)
                alert1_dataframe = pd.merge(pd.merge(sensors_names, open_sensor, on="sensor_id"), alert1, on="sensor_id")
                alert2_dataframe = pd.merge(pd.merge(sensors_names, open_sensor, on="sensor_id"), alert2, on="sensor_id")
                alert3_dataframe = pd.merge(pd.merge(sensors_names, open_sensor, on="sensor_id"), alert3, on="sensor_id")
                print_alerts(alert1_dataframe, 1)
                print_alerts(alert2_dataframe, 2)
                print_alerts(alert3_dataframe, 3)
            elif choice == 6:
                break
            else:
                print("Error: choice not valid")
            print("\nPress enter to continue..")
            input()

    print("Quitting program..")
    cnx.close()

if __name__ == "__main__":
    main()
