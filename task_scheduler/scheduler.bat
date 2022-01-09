set PATH="D:\PycharmProjects\Python\Covid-19\task_scheduler\log.txt"
echo Update daily data >> %PATH%
echo Beginning time = %time% %date% >> %PATH%
"C:\Users\My PC\AppData\Local\Programs\Python\Python39\python.exe" "D:\PycharmProjects\Python\Covid-19\task_scheduler\crawl_data.py" >> %PATH%
echo Ending time = %time% %date% >> %PATH%
echo. >> %PATH%