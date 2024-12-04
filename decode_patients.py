import pandas as pd

import psycopg2

conn = psycopg2.connect(database = "db",
                        host =     "rc1b-o3ezvcgz5072sgar.mdb.yandexcloud.net",
                        user =     "hseguest",
                        password = "hsepassword",
                        port =     "6432")

conn.autocommit = False


def filter_patients(df: pd.DataFrame, test_count: int = 2) -> pd.DataFrame:
    patient_code = "Код пациента"
    groups = df.groupby(patient_code).size()
    pat_codes = groups[groups >= test_count]
    return df[df[patient_code].isin(pat_codes.index)]

def clean_values(df: pd.DataFrame) -> pd.DataFrame:
    values_voc = {"+": "п", "-": "о"}
    value_name = "Значение"
    df = df.copy()
    df[value_name] = df[value_name].astype("str").str.lower()
    for rep, pat in values_voc.items():
        mask = df[value_name].str.startswith(pat)
        df.loc[df[mask].index, value_name] = rep
    return df.reset_index(drop=True)

def get_med_an_name(conn) -> pd.DataFrame:
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM de.med_an_name")
        records = cursor.fetchall()
        df = pd.DataFrame(records)
    return df

def check_range(df: pd.DataFrame, med_df: pd.DataFrame) -> pd.DataFrame:
    check_name = "Повышен"
    test_name = "Анализ"
    value_name = "Значение"
    med_df = med_df.copy()
    df = df.copy()
    med_df = med_df[med_df.loc[:, 2] == "N"]
    unique_test_names = med_df[0].unique()
    df = df[df[test_name].isin(unique_test_names)]

    df[check_name] = None
    for i, row in df.iterrows():
        min_val = med_df[med_df[0] == row[test_name]][3].values[0]
        max_val = med_df[med_df[0] == row[test_name]][4].values[0]
        curr_val = float(row[value_name])
        if max_val < curr_val:
            df.loc[i, check_name] = True
        elif min_val > curr_val:
            df.loc[i, check_name] = False
    return df




if __name__ == "__main__":
    df = pd.read_excel("data/medicine.xlsx", sheet_name="hard", header=0)
    df = filter_patients(df)
    df = clean_values(df)
    print(df)
    med_df = get_med_an_name(conn)
    df = check_range(df, med_df)
    print(df)
    conn.close()