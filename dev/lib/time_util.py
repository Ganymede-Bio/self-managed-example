from datetime import date, datetime

def print_date():
    print(date.today().strftime("%Y-%m-%d"))

def print_date_time():
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def print_date_time_micros():
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
