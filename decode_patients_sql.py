import pandas as pd
import psycopg2

# Приводим строковые значения в столбце 'Значение' к общему виду типа '+' или '-'
SQL_CLEAN_VALUES = """
CREATE TEMP TABLE cleaned_data AS
SELECT
    "Код пациента",
    "Анализ",
    CASE
        WHEN "Значение" ~ '^П' THEN '+'
        WHEN "Значение" ~ '^О' THEN '-'
        ELSE "Значение"
    END AS Значение
FROM medicine;
"""

# Проверяем лежит ли значение по анализу в нужном интервале, на основе этого выдаем заключение
SQL_DECODE_ANALYSIS = """
CREATE TEMP TABLE decoded_data AS
SELECT
    cd."Код пациента",
    cd."Анализ",
    man.name AS "Название анализа",
    cd.Значение,
    man.min_value AS "Мин.значение",
    man.max_value AS "Макс.значение",
    CASE
        WHEN cd.Значение = '+' THEN 'Положительный'
        WHEN cd.Значение = '-' THEN 'Отрицательный'
        WHEN cd.Значение ~ '^[0-9.]+$' AND CAST(cd.Значение AS FLOAT) < man.min_value THEN 'Понижен'
        WHEN cd.Значение ~ '^[0-9.]+$' AND CAST(cd.Значение AS FLOAT) > man.max_value THEN 'Повышен'
        WHEN cd.Значение ~ '^[0-9.]+$' THEN 'Норма'
        ELSE 'Некорректное значение'
    END AS Заключение
FROM cleaned_data cd
JOIN de.med_an_name man ON cd."Анализ" = man.id;
"""

# Проверяем есть ли некорректные значения в столбце 'Значение'
SQL_CHECK_CORRECT_VALUE = """
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM decoded_data WHERE Заключение = 'Некорректное значение') THEN
        RAISE EXCEPTION 'Обнаружены некорректные значения в данных';
    END IF;
END $$;
"""

# Оставляем пациентов с 2 или более плохими анализами
SQL_FILTER_PATIENTS = """
CREATE TEMP TABLE filtered_patients AS
SELECT
    "Код пациента",
    COUNT(*) AS "Всего тестов",
    SUM(CASE WHEN Заключение IN ('Повышен', 'Понижен', 'Положительный') THEN 1 ELSE 0 END) AS "Плохие анализы"
FROM decoded_data
GROUP BY "Код пациента"
HAVING SUM(CASE WHEN Заключение IN ('Повышен', 'Понижен', 'Положительный') THEN 1 ELSE 0 END) >= 2;
"""

# Получаем финальную таблицу - телефон, имя, название анализа и заключение
SQL_FINAL_RESULTS_WITH_PATIENTS = """
SELECT
    mn.phone AS Телефон,
    mn.name AS Имя,
    cd."Название анализа",
    cd.Заключение AS заключение
FROM filtered_patients fp
JOIN decoded_data cd ON fp."Код пациента" = cd."Код пациента"
JOIN de.med_name mn ON fp."Код пациента" = mn.id
WHERE cd.Заключение IN ('Повышен', 'Понижен', 'Положительный');
"""

input_file = "data/medicine.xlsx"
df = pd.read_excel(input_file, sheet_name="hard", header=0)

# Подключение к базе данных
conn = psycopg2.connect(
    database="db",
    host="rc1b-o3ezvcgz5072sgar.mdb.yandexcloud.net",
    user="hseguest",
    password="hsepassword",
    port="6432",
)
conn.autocommit = False


with conn.cursor() as cursor:
    # Создаем временную таблицу medicine
    cursor.execute(
        """
        CREATE TEMP TABLE medicine (
            "Код пациента" INT,
            "Анализ" TEXT,
            "Значение" TEXT
        );
        """
    )

    # Загружаем данные из medicine.xlsx во временную таблицу medicine
    for _, row in df.iterrows():
        value = str(row["Значение"]).replace(",", ".")  # Заменяем запятые на точки
        cursor.execute(
            """
            INSERT INTO medicine ("Код пациента", "Анализ", "Значение")
            VALUES (%s, %s, %s);
            """,
            (row["Код пациента"], row["Анализ"], value),
        )

    # Обрабатываем данные через SQL-запросы
    cursor.execute(SQL_CLEAN_VALUES)
    cursor.execute(SQL_DECODE_ANALYSIS)
    cursor.execute(SQL_CHECK_CORRECT_VALUE)
    cursor.execute(SQL_FILTER_PATIENTS)
    cursor.execute(SQL_FINAL_RESULTS_WITH_PATIENTS)

    results = cursor.fetchall()

    # Сохраняем данные из временной таблицы medicine в постоянную таблицу public.maka_medicine в бд
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS public.maka_med_results AS
        SELECT
            mn.phone AS Телефон,
            mn.name AS Имя,
            cd."Название анализа" AS Анализ,
            cd.Заключение AS Заключение
        FROM filtered_patients fp
        JOIN decoded_data cd ON fp."Код пациента" = cd."Код пациента"
        JOIN de.med_name mn ON fp."Код пациента" = mn.id
        WHERE cd.Заключение IN ('Повышен', 'Понижен', 'Положительный');
        """
    )

    conn.commit()

    # Сохраняем окончательные результаты в Excel
    output_file = "data/maka_med_results.xlsx"

    columns = ["Телефон", "Имя", "Анализ", "Заключение"]
    final_df = pd.DataFrame(results, columns=columns)
    final_df.to_excel(output_file, index=False)

conn.close()
