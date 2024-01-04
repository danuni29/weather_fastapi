import pandas as pd
import requests

def main():

    start_year = 2004
    end_year = 2023


    filename = f"buan_{start_year}_{end_year}.csv"
    URL = f"https://api.taegon.kr/stations/243/?sy={start_year}&ey={end_year}&format=csv"
    res = requests.get(URL)
    with open(filename, "w", newline="") as f:
        f.write(res.text)


    buan_df = pd.read_csv('buan_2004_2023.csv')

    buan_df.columns = buan_df.columns.str.strip()
    # print(buan_df.columns)

    buan_df = buan_df.loc[~buan_df.month.isin([7, 8, 9]), ['year', 'month', 'day', 'tavg', 'rainfall']]


    # buan_df['date'] = buan_df['month'] + buan_df['day']

    # zfill -> 자리수 맞춰줌
    buan_df['date'] = buan_df['month'].astype(str).str.zfill(2) + buan_df['day'].astype(str).str.zfill(2)

    group_df = buan_df.groupby('date')

    # print(group_df.count())
    # df = group_df.tavg.quantile([0.25, 0.75])
    df = group_df['tavg'].describe()[['25%', '75%']]
# -------------------

    rain_df = buan_df.groupby(['year', 'month'])['rainfall'].sum()
    print(rain_df)
    rain_df = rain_df.groupby('month')
    print(rain_df.describe()[['25%', '75%']])


# -------------------
    start_year = 2023
    end_year = 2024
    filename = f"buan_{start_year}_{end_year}.csv"
    URL = f"https://api.taegon.kr/stations/243/?sy={start_year}&ey={end_year}&format=csv"

    res = requests.get(URL)
    with open(filename, "w", newline="") as f:
        f.write(res.text)


    today_df = pd.read_csv('buan_2023_2024.csv')
    today_df.columns = today_df.columns.str.strip()
    print(today_df.columns)

    today_df['date'] = today_df['year'].astype(str)+today_df['month'].astype(str).str.zfill(2)+today_df['day'].astype(str).str.zfill(2)

    today_df['date'] = pd.to_datetime(today_df['date'])
    cut_date = pd.to_datetime('2023-10-01')

    cut_today_df = today_df[today_df['date'] >= cut_date]
    cut_today_df = cut_today_df.set_index('date', drop=False)

    # print(cut_today_df['date'])
# --------------
#     print(cut_today_df.groupby('month')['rainfall'].sum())
    cut_today_df['end_of_month'] = cut_today_df['date'] + pd.offsets.MonthEnd(0) == cut_today_df['date']
    # print(cut_today_df)
    # end_of_month에 True값이 적어도 1개 있는지 판별
    # 1개 있다면 끝난 월
    for month, group in cut_today_df.groupby('month'):
        if group['end_of_month'].any():
            print(group['rainfall'].sum())


# ---------------

    # 익산 -> 군산(140)으로 대체
    start_year = 2004
    end_year = 2023
    filename = f"iksan_{start_year}_{end_year}.csv"
    URL = f"https://api.taegon.kr/stations/140/?sy={start_year}&ey={end_year}&format=csv"

    res = requests.get(URL)
    with open(filename, "w", newline="") as f:
        f.write(res.text)









if __name__ == '__main__':
    main()