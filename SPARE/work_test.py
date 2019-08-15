import datetime as dt


bt=dt.datetime(1955, 1, 1)
print( (bt + 452*dt.timedelta(days=31)).replace(day=1))

