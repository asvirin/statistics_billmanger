from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime, date, time, timedelta
import seaborn as sns; sns.set()
import matplotlib.pyplot as plt
import sys
import getopt

DATABASE_HOST = os.environ['DATABASE_HOST']
DATABASE_USER = os.environ['DATABASE_USER']
DATABASE_PASSWORD = os.environ['DATABASE_PASSWORD']
DATABASE_NAME = os.environ['DATABASE_NAME']
DATABASE_CHARSET = os.environ['DATABASE_CHARSET']

SQL_USERS = f'SELECT id, name, registration_date \
                FROM billmgr.account'

SQL_ITEMS = "SELECT account, createdate, expiredate, name \
                FROM billmgr.item"

def get_heatmap(df, start_date, end_date):
    plt.subplots(figsize=(16,9), dpi=72)
    plt.title('Retention map, billmanager', fontsize = 16)
    ax = sns.heatmap(df, annot=True, fmt=".0%")
    plt.savefig(f"Retention map {start_date} - {end_date}.png", dpi=300)
    plt.show()

def diff_month(start_date, end_date):
    return (start_date.year - end_date.year) * 12 + start_date.month - end_date.month

def get_retention_map(start_dt, end_dt, engine):
    start_date = datetime.strptime(start_dt, '%Y-%m-%d')
    end_date = datetime.strptime(end_dt, '%Y-%m-%d')
    
    count_cohort = diff_month(end_date, start_date)
    
    cohorts_dict = []
    
    all_users_df = pd.read_sql_query(SQL_USERS, engine)
    items_df = pd.read_sql_query(SQL_ITEMS, engine)
    
    for cohort in range(count_cohort):
        start_month = start_date.replace(day=1)
        if start_month.month == 12:
            last_day_previos_month = start_month.replace(year=start_month.year+1).replace(month=1) - timedelta (days = 1)
        else:  
            last_day_previos_month = start_month.replace(month=start_month.month+1) - timedelta (days = 1)

        list_users = all_users_df.query('(registration_date >= @start_month)&(registration_date <= @last_day_previos_month)')['id'].tolist()
        if start_month.month==12:
            start_month_cohort = start_month.replace(year=start_month.year+1).replace(month=1)
        else:
            start_month_cohort = start_month.replace(month=start_month.month+1)
        count_month = diff_month(end_date, start_month)
        for cohort_id in range(count_month):
            start_month_cohort_date = datetime.date(start_month_cohort)
            items_df_cohort = items_df[items_df['account'].isin(list_users)]
            items_df_cohort = items_df_cohort.query('expiredate > @start_month_cohort_date')
            if start_month_cohort.month == 12:
                start_month_cohort = start_month_cohort.replace(year=start_month_cohort.year+1).replace(month=1) 
            else:
                start_month_cohort = start_month_cohort.replace(month=start_month_cohort.month+1)
            cohorts_dict.append([datetime.date(start_month).strftime('%Y-%m'), cohort, cohort_id, len(items_df_cohort['account'].unique())])

        if start_month.month == 12:
            start_date = start_month.replace(year=start_month.year+1).replace(month=1) 
        else:   
            start_date = start_month.replace(month=start_month.month+1)
            
            
    cohorts_df = pd.DataFrame(cohorts_dict)
    cohorts_df.columns = ['Дата регистрации', 1, 'Срок использования, месяцы', 'Процент оставшихся']
    cohorts_df = pd.pivot_table(cohorts_df, values='Процент оставшихся', index='Дата регистрации', columns='Срок использования, месяцы')
    cohorts_df = cohorts_df.div(cohorts_df[0], axis=0)
    
    get_heatmap(cohorts_df, start_date, end_date)

if __name__ == "__main__":
    # Задаём формат входных параметров
    unixOptions = "s:e:"  
    gnuOptions = ["start_dt=", "end_dt="]

    # Получаем строку входных параметров
    fullCmdArguments = sys.argv
    argumentList = fullCmdArguments[1:]

    # Проверяем входные параметры на соответствие формату,
    # заданному в unixOptions и gnuOptions
    try:  
        arguments, values = getopt.getopt(argumentList, unixOptions, gnuOptions)
    except getopt.error as err:  
        print (str(err))
        sys.exit(2)      # Прерываем выполнение, если входные параметры некорректны

    # Считываем значения из строки входных параметров
    start_dt = ''
    end_dt = ''   
    for currentArgument, currentValue in arguments:  
        if currentArgument in ("-s", "--start_dt"):
            start_dt = currentValue                                   
        elif currentArgument in ("-e", "--end_dt"):
            end_dt = currentValue
            
    engine = create_engine(f"mysql+pymysql://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}", 
                           encoding=f'{DATABASE_CHARSET}')
    
    get_retention_map(start_dt, end_dt, engine)
