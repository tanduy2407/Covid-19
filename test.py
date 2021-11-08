from datetime import date, timedelta
last_90_days = (date.today()-timedelta(days=15)).strftime('%m/%d/%Y')
print(last_90_days)