from datetime import datetime
from datetime import date
from datetime import timedelta
import datetime as dt

yesterday = dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=2) - dt.timedelta(days=1)
print(yesterday.strftime("%Y-%m-%d %H:%M:%S"))


# string = f"TEST 24\n"
# print(string[:-1])

# gaarsdagensdato = date.today() - timedelta(days = 1)
# print(gaarsdagensdato)

#current_time = date.today() #now(timezone.utc)
# current_time =datetime.datetime.now(timezone.utc) + datetime.timedelta(hours=2)
# print("first ",current_time)


#tim = datetime.datetime.now(timezone.utc).replace(hour = 0, minute = 0, second = 0, microsecond = 0) + datetime.timedelta(hours=2)
#print(tim)

# today = date.today()
# yesterday = date.today() - timedelta(days = 1)
# print(str(today) + " & " + str(yesterday))

# string = ""

# for i in range(10):
#     string = "\n".join("Hei!")

# print(string)


# yesterday = datetime.dt.now(datetime.timezone.utc).replace(hour = 0, minute = 0, second = 0, microsecond = 0) - datetime.timedelta(days=1) - datetime.timedelta(hours=2)
# today = datetime.dt.now(datetime.timezone.utc).replace(hour = 0, minute = 0, second = 0, microsecond = 0) - datetime.timedelta(hours=2)
# print(str(today) + " & " + str(yesterday))

# def get_periode():
#     """
#     henter periode for the tidligere måneden eksample--> i dag er 19.04.2022, metoden vil kalkulerer periode aarMaaned eks) '202203'
#     :param periode:
#     :return: periode
#     """
#     today = datetime.date.today() # dato for idag 2022-04-19
#     first = today.replace(day=1) # dato for første dag i måneden 2022-04-01
#     lastMonth = first - datetime.timedelta(days=1) # dato for siste dag i tidligere måneden

#     return lastMonth.strftime("%Y%m") # henter bare aar og maaned

# periode = get_periode()
# print(periode)

