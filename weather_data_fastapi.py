import requests
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd

app = FastAPI()




@app.get("/buan/temp")
def buan_temp():
    buan_df = pd.read_csv('buan_2004_2023.csv')

    buan_df.columns = buan_df.columns.str.strip()
    print(buan_df.columns)

    buan_df = buan_df.loc[~buan_df.month.isin([7, 8, 9]), ['year', 'month', 'day', 'tavg', 'rainfall']]

    # buan_df['date'] = buan_df['month'] + buan_df['day']

    # zfill -> 자리수 맞춰줌
    buan_df['date'] = buan_df['month'].astype(str).str.zfill(2) + buan_df['day'].astype(str).str.zfill(2)

    group_df = buan_df.groupby('date')

    # print(group_df.count())

    # df = group_df.tavg.quantile([0.25, 0.75])
    result = group_df['tavg'].describe()[['25%', '75%']]

    return result

@app.get("/buan/temp/{date}")
def buan_temp_by_day(date : str):
    buan_df = pd.read_csv('buan_2004_2023.csv')

    buan_df.columns = buan_df.columns.str.strip()
    print(buan_df.columns)

    buan_df = buan_df.loc[~buan_df.month.isin([7, 8, 9]), ['year', 'month', 'day', 'tavg', 'rainfall']]

    # buan_df['date'] = buan_df['month'] + buan_df['day']

    # zfill -> 자리수 맞춰줌
    buan_df['date'] = buan_df['month'].astype(str).str.zfill(2) + buan_df['day'].astype(str).str.zfill(2)

    filtered_df = buan_df[buan_df['date'] == date]

    result = filtered_df['tavg'].describe()[['25%', '75%']].to_dict()

    return result




@app.get("/iksan/temp")
def iksan_temp():
    iksan_df = pd.read_csv('iksan_2004_2023.csv')

    iksan_df.columns = iksan_df.columns.str.strip()

    iksan_df = iksan_df.loc[~iksan_df.month.isin([7, 8, 9]), ['year', 'month', 'day', 'tavg', 'rainfall']]

    # buan_df['date'] = buan_df['month'] + buan_df['day']

    # zfill -> 자리수 맞춰줌
    iksan_df['date'] = iksan_df['month'].astype(str).str.zfill(2) + iksan_df['day'].astype(str).str.zfill(2)

    group_df = iksan_df.groupby('date')

    # print(group_df.count())

    # df = group_df.tavg.quantile([0.25, 0.75])
    df = group_df['tavg'].describe()[['25%', '75%']]

    return df

@app.get("/iksan/temp/{date}")
def iksan_temp_by_day(date: str):
    iksan_df = pd.read_csv('iksan_2004_2023.csv')

    iksan_df.columns = iksan_df.columns.str.strip()

    iksan_df = iksan_df.loc[~iksan_df.month.isin([7, 8, 9]), ['year', 'month', 'day', 'tavg', 'rainfall']]

    # buan_df['date'] = buan_df['month'] + buan_df['day']

    # zfill -> 자리수 맞춰줌
    iksan_df['date'] = iksan_df['month'].astype(str).str.zfill(2) + iksan_df['day'].astype(str).str.zfill(2)

    filtered_df = iksan_df[iksan_df['date'] == date]

    result = filtered_df['tavg'].describe()[['25%', '75%']].to_dict()

    return result


@app.get("/buan/rainfall")
def buan_rainfall():

    buan_df = pd.read_csv('buan_2004_2023.csv')

    buan_df.columns = buan_df.columns.str.strip()
    # print(buan_df.columns)

    buan_df = buan_df.loc[~buan_df.month.isin([7, 8, 9]), ['year', 'month', 'day', 'tavg', 'rainfall']]

    rain_df = buan_df.groupby(['year', 'month'])['rainfall'].sum()
    # print(rain_df)
    rain_df = rain_df.groupby('month')
    # print(rain_df.describe()[['25%', '75%']])

    return round(rain_df.describe()[['25%', '75%']], 2)



@app.get("/iksan/rainfall")
def iksan_rainfall():

    iksan_df = pd.read_csv('iksan_2004_2023.csv')

    iksan_df.columns = iksan_df.columns.str.strip()
    # print(buan_df.columns)

    iksan_df = iksan_df.loc[~iksan_df.month.isin([7, 8, 9]), ['year', 'month', 'day', 'tavg', 'rainfall']]

    rain_df = iksan_df.groupby(['year', 'month'])['rainfall'].sum()
    # print(rain_df)
    rain_df = rain_df.groupby('month')
    # print(rain_df.describe()[['25%', '75%']])

    return round(rain_df.describe()[['25%', '75%']], 2)

@app.get("/buan/today/temp")
def buan_today_temp():

    start_year = 2023
    end_year = 2024
    filename = f"buan_{start_year}_{end_year}.csv"
    URL = f"https://api.taegon.kr/stations/243/?sy={start_year}&ey={end_year}&format=csv"

    res = requests.get(URL)
    with open(filename, "w", newline="") as f:
        f.write(res.text)

    today_df = pd.read_csv('buan_2023_2024.csv')
    today_df.columns = today_df.columns.str.strip()

    today_df['date'] = today_df['year'].astype(str)+today_df['month'].astype(str).str.zfill(2)+today_df['day'].astype(str).str.zfill(2)
    today_df['date'] = pd.to_datetime(today_df['date'])
    cut_date = pd.to_datetime('2023-10-01')

    cut_today_df = today_df[today_df['date'] >= cut_date]
    cut_today_df = cut_today_df.set_index('date', drop=False)
    cut_today_df['date'] = cut_today_df['date'].dt.strftime('%Y-%m-%d')
    # return cut_today_df['tavg']
    return cut_today_df[['date', 'tavg']].set_index('date').to_dict()['tavg']


@app.get("/iksan/today/temp")
def iksan_today_temp():
    start_year = 2023
    end_year = 2024
    filename = f"iksan_{start_year}_{end_year}.csv"
    URL = f"https://api.taegon.kr/stations/140/?sy={start_year}&ey={end_year}&format=csv"

    res = requests.get(URL)
    with open(filename, "w", newline="") as f:
        f.write(res.text)


    today_df = pd.read_csv('iksan_2023_2024.csv')
    today_df.columns = today_df.columns.str.strip()

    today_df['date'] = today_df['year'].astype(str)+today_df['month'].astype(str).str.zfill(2)+today_df['day'].astype(str).str.zfill(2)
    today_df['date'] = pd.to_datetime(today_df['date'])
    cut_date = pd.to_datetime('2023-10-01')

    cut_today_df = today_df[today_df['date'] >= cut_date]
    cut_today_df = cut_today_df.set_index('date', drop=False)
    cut_today_df['date'] = cut_today_df['date'].dt.strftime('%Y-%m-%d')
    # return cut_today_df['tavg']
    return cut_today_df[['date', 'tavg']].set_index('date').to_dict()['tavg']

@app.get("/buan/today/rainfall")
def buan_today_rainfall():
    start_year = 2023
    end_year = 2024
    filename = f"buan_{start_year}_{end_year}.csv"
    URL = f"https://api.taegon.kr/stations/243/?sy={start_year}&ey={end_year}&format=csv"

    res = requests.get(URL)
    with open(filename, "w", newline="") as f:
        f.write(res.text)
    today_df = pd.read_csv('buan_2023_2024.csv')
    today_df.columns = today_df.columns.str.strip()

    today_df['date'] = today_df['year'].astype(str) + today_df['month'].astype(str).str.zfill(2) + today_df['day'].astype(str).str.zfill(2)
    today_df['date'] = pd.to_datetime(today_df['date'])
    cut_date = pd.to_datetime('2023-10-01')

    cut_today_df = today_df[today_df['date'] >= cut_date]
    cut_today_df = cut_today_df.set_index('date', drop=False)
    # cut_today_df['date'] = cut_today_df['date'].dt.strftime('%Y-%m-%d')

    cut_today_df['end_of_month'] = cut_today_df['date'] + pd.offsets.MonthEnd(0) == cut_today_df['date']

    monthly_rainfall_dict = {}

    for month, group in cut_today_df.groupby('month'):
        if group['end_of_month'].any():
            # sum_rainfall.append(group['rainfall'].sum())
            # sum_rainfall.append({'month': month, 'rainfall_sum': group['rainfall'].sum()})
            monthly_rainfall_sum = round(group['rainfall'].sum(), 2)
            monthly_rainfall_dict[month] = monthly_rainfall_sum


    return monthly_rainfall_dict

@app.get("/iksan/today/rainfall")
def iksan_today_rainfall():

    start_year = 2023
    end_year = 2024
    filename = f"iksan_{start_year}_{end_year}.csv"
    URL = f"https://api.taegon.kr/stations/140/?sy={start_year}&ey={end_year}&format=csv"

    res = requests.get(URL)
    with open(filename, "w", newline="") as f:
        f.write(res.text)

    today_df = pd.read_csv('iksan_2023_2024.csv')
    today_df.columns = today_df.columns.str.strip()

    today_df['date'] = today_df['year'].astype(str) + today_df['month'].astype(str).str.zfill(2) + today_df['day'].astype(str).str.zfill(2)
    today_df['date'] = pd.to_datetime(today_df['date'])
    cut_date = pd.to_datetime('2023-10-01')

    cut_today_df = today_df[today_df['date'] >= cut_date]
    cut_today_df = cut_today_df.set_index('date', drop=False)
    # cut_today_df['date'] = cut_today_df['date'].dt.strftime('%Y-%m-%d')

    cut_today_df['end_of_month'] = cut_today_df['date'] + pd.offsets.MonthEnd(0) == cut_today_df['date']

    monthly_rainfall_dict = {}

    for month, group in cut_today_df.groupby('month'):
        if group['end_of_month'].any():
            # sum_rainfall.append(group['rainfall'].sum())
            # sum_rainfall.append({'month': month, 'rainfall_sum': group['rainfall'].sum()})
            monthly_rainfall_sum = round(group['rainfall'].sum(), 2)
            monthly_rainfall_dict[month] = monthly_rainfall_sum


    return monthly_rainfall_dict






uvicorn.run(app, host="127.0.0.1", port=8000)