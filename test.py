from datetime import date, timedelta
last_90_days = (date.today()-timedelta(days=15)).strftime('%m/%d/%Y')
print(last_90_days)

# import pandas as pd

# a = pd.date_range('10/10/2021' ,'11/9/2021', freq='10D')

# for i in range(len(a)-1):

#     print(a[i].strftime('%m/%d/%Y'))
#     print((a[i+1]-timedelta(days=1)).strftime('%m/%d/%Y'))
#     print('-------------------')
