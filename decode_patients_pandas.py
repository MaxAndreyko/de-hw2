import pandas as pd
import psycopg2


def filter_patients(df: pd.DataFrame, test_count: int = 2) -> pd.DataFrame:
    """
    Filter patients based on the number of tests they have.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing patient data with a column for patient codes.
    test_count : int, optional
        Minimum number of tests a patient must have to be included in the result (default is 2).

    Returns
    -------
    pd.DataFrame
        A DataFrame containing only the patients who have at least `test_count` tests.
    """
    patient_code = "Код пациента"
    groups = df.groupby(patient_code).size()
    pat_codes = groups[groups >= test_count]
    return df[df[patient_code].isin(pat_codes.index)]


def clean_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize the values in the specified column of the DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing a column named 'Значение' with test results.

    Returns
    -------
    pd.DataFrame
        A DataFrame with standardized values in the 'Значение' column.
    """
    values_voc = {"Положительный": ("п", "+"), "Отрицательный": ("о", "-")}
    value_name = "Значение"
    df = df.copy()
    df[value_name] = df[value_name].astype("str").str.lower()

    for rep, pat in values_voc.items():
        mask = df[value_name].str.startswith(pat)
        df.loc[df[mask].index, value_name] = rep

    return df.reset_index(drop=True)


def get_med_an_name(conn) -> pd.DataFrame:
    """
    Retrieve medical analysis names from the database.

    Parameters
    ----------
    conn : psycopg2 connection object
        Connection to the PostgreSQL database.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing records from the 'de.med_an_name' table.
    """
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM de.med_an_name")
        records = cursor.fetchall()
        df = pd.DataFrame(records)

    return df


def get_med_name(conn) -> pd.DataFrame:
    """
    Retrieve medical names from the database.

    Parameters
    ----------
    conn : psycopg2 connection object
        Connection to the PostgreSQL database.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing records from the 'de.med_name' table.
    """
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM de.med_name")
        records = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(records, columns=columns)

    return df


def check_range(
    df: pd.DataFrame, med_df: pd.DataFrame, test_count: int = 2
) -> pd.DataFrame:
    """
    Check if test results fall within specified ranges and categorize them.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing test results with columns 'Заключение', 'Анализ', and 'Значение'.

    med_df : pd.DataFrame
        DataFrame containing medical ranges for each analysis type.

    test_count : int
        Minimum number of tests a patient must have to be included in the result (default is 2).

    Returns
    -------
    pd.DataFrame
        A DataFrame containing filtered results based on analysis ranges.

        The resulting DataFrame includes only patients with significant deviations from normal ranges.

        Columns include 'Код пациента', 'Анализ', and 'Заключение'.
    """

    check_name = "Заключение"
    test_name = "Анализ"
    value_name = "Значение"
    patient_name = "Код пациента"

    df[check_name] = None

    numeric_mask = df[value_name].apply(lambda x: x.replace(".", "", 1).isdigit())

    # Assign non-numeric values directly to Заключение
    df.loc[~numeric_mask, check_name] = df.loc[~numeric_mask, value_name]

    # Iterate through numeric rows to check ranges and assign conclusions
    for i, row in df[numeric_mask].iterrows():
        value = row[value_name]
        curr_val = float(value)

        # Get min and max values for current analysis type from med_df
        min_val = med_df[med_df[0] == row[test_name]][3].values[0]
        max_val = med_df[med_df[0] == row[test_name]][4].values[0]

        # Determine if current value is above, below or within normal range
        if max_val < curr_val:
            df.loc[i, check_name] = "Повышен"
        elif min_val > curr_val:
            df.loc[i, check_name] = "Понижен"
        elif min_val <= curr_val <= max_val:
            df.loc[i, check_name] = "Норма"

    # Group by patient code to count tests and identify abnormal results
    grouped = (
        df.groupby(patient_name, as_index=False)
        .agg(
            {
                test_name: "count",
                check_name: lambda x: ((x != "Норма") & (x != "Отрицательный")).sum(),
            }
        )
        .rename(
            columns={test_name: "Количество анализов", check_name: "Плохие анализы"}
        )
    )

    grouped = grouped[grouped["Плохие анализы"] >= test_count]

    filtered_df = df[df[patient_name].isin(grouped[patient_name])]

    final_table = filtered_df[
        filtered_df[check_name].isin(["Повышен", "Понижен", "Положительный"])
    ][[patient_name, test_name, check_name]]

    return final_table


def get_final_table(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """
    Combine two DataFrames into a final result set for reporting.

    Parameters
    ----------
    df1 : pd.DataFrame
        The first DataFrame containing patient data with a column for patient codes.

    df2 : pd.DataFrame
        The second DataFrame containing medical names associated with patient codes.

    Returns
    -------
    pd.DataFrame
        A final DataFrame containing columns for phone number, name, analysis type,
        and conclusion about the patient's health status.
    """
    df1 = df1.rename(columns={"Код пациента": "id"})
    df_final = (
        df1.merge(df2, how="inner", on="id")
        .drop(columns=["id"])
        .rename(columns={"name": "Имя", "phone": "Телефон"})
    )

    df_final = df_final[["Телефон", "Имя", "Анализ", "Заключение"]]

    return df_final


def load_final_table_to_db(conn, df: pd.DataFrame) -> None:
    """
    Load data from a Pandas DataFrame into a PostgreSQL database.

    Parameters
    ----------
    conn : psycopg2 connection object
        Connection to the PostgreSQL database.

    df : pd.DataFrame
        DataFrame containing the data to be inserted into the database table.

    Returns
    -------
    None
        This function does not return any value. It performs an insert operation into the database.
    """
    table_name = "public.maka_med_results"

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        Телефон VARCHAR(20),
        Имя VARCHAR(100),
        Анализ VARCHAR(20),
        Заключение VARCHAR(50)
    );
    """

    insert_query = f"""
    INSERT INTO {table_name}  (Телефон, Имя, Анализ, Заключение)
    VALUES (%s, %s, %s, %s);
    """
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
            data_tuples = list(df.itertuples(index=False, name=None))
            cursor.executemany(insert_query, data_tuples)


if __name__ == "__main__":
    conn = psycopg2.connect(
        database="db",
        host="rc1b-o3ezvcgz5072sgar.mdb.yandexcloud.net",
        user="hseguest",
        password="hsepassword",
        port="6432",
    )

    conn.autocommit = False
    df = pd.read_excel("data/medicine.xlsx", sheet_name="hard", header=0)
    df = filter_patients(df)
    df = clean_values(df)
    med_df = get_med_an_name(conn)
    med_name_df = get_med_name(conn)
    df = check_range(df, med_df)
    df_final = get_final_table(df, med_name_df)
    load_final_table_to_db(conn, df_final)
    df_final.to_excel("data/maka_med_results.xlsx")
    conn.close()
