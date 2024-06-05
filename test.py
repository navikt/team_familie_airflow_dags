import datetime
from datetime import timedelta,timezone, date, datetime



#current_time = date.today() #now(timezone.utc)
# current_time =datetime.datetime.now(timezone.utc) + datetime.timedelta(hours=2)
# print("first ",current_time)


#tim = datetime.datetime.now(timezone.utc).replace(hour = 0, minute = 0, second = 0, microsecond = 0) + datetime.timedelta(hours=2)
#print(tim)

today = datetime.now()
yesterday = datetime.now() #- datetime.timedelta(days=1)
print(str(today) + " & " + str(yesterday))

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

