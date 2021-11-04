from datetime import date, timedelta
last_90_days = (date.today()-timedelta(days=90)).strftime('%m/%d/%Y')
print(last_90_days)