import datetime
from datetime import timedelta
import pandas as pd
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

from modules import config
from modules.pg_connector import PGConnector

"""
DAG, реализующий тестовое задание на позицию Data Engineer в компанию SkyEng

Общая конфирурация процесса и класс, реализующий доступ к БД, вынесены в подпапку modules

Структура DAG'а.
Реализовано три задачи, в каждой в качестве оператора использован PythonOperator.
Задачи выполняются последовательно, следующая выполняется при условии успешного выполнения предыдущей, если есть

extract_raw_data >> load_to_dwh_temp >> load_to_dwh_target

При выполнении задания приняты следующие допущения и ограничения:
1. И исходные данные и целевые таблицы DWH расположены в СУБД Postgres
2. В качестве файлового хранилища используется локальная файловая система
3. Сделаны только основные проверки на возможные проблемы. Не проверяются случаи:
    3.1. Повреждение файла-источника
    3.2. Некорректная структура таблиц БД
    3.3. Несоответствие типов данных
    3.4. Отсутствие данных в таблицах
"""

# Добавляем подпапку modules в переменную окружения path для импорта
# Вероятно, первый запуск DAG'а будет неуспешным из-за неудачного импорта.
current_dir_path = os.getcwd()
modules_path = os.path.join(current_dir_path, 'modules')
sys.path.insert(0, modules_path)

# Параметры DAG'а по умолчанию
default_args = {
    'owner': 'alexx',
    'depends_on_past': True,
    'email': ['alexx.ru@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Задаём столбцы таблиц с данными
# Для сырой структуры данных берём первые элементы маппинга, если они не None
raw_data_columns = [pair[0] for pair in config.mapping if pair[0] is not None]

# Для целевой структуры данных берём вторые элементы маппинга, если они не None
target_data_columns = [pair[1] for pair in config.mapping if pair[1] is not None]

# Для последующей трансформации данных формируем словарь перевода столбцов, пропуская все элементы с None
no_none_mapping = [pair for pair in config.mapping if None not in pair]
mapping_dict = dict(no_none_mapping)

# Выгружаем путь к папке и имя файла временного хранилища
temp_folder_path = config.temp_folder_path
temp_filename = config.temp_filename

# Задаём полный путь к файлу временного хранилища
raw_data_csv_path = os.path.join(temp_folder_path, temp_filename)


def extract_from_postgres_to_csv():
    """
    Функция для извлечения сырых данных и сохранения их в файловом хранилище
    :return: None
    """

    # Пробуем установить соединение с БД источника (выбран postgres)
    with PGConnector(dbname=config.source_dbname,
                     dbuser=config.source_dbuser,
                     dbpass=config.source_dbpass,
                     dbhost=config.source_dbhost,
                     dbport=config.source_dbport) as pg_connector:

        # Если соедниение с БД установлено выгружаем данные
        if pg_connector.conn:
            print('Есть соединение с БД')
            raw_data = pg_connector.select(f'SELECT {", ".join(raw_data_columns)} FROM {config.source_schema}.{config.source_table}')
        else:
            raise ConnectionError('Нет соединения с БД')

    # Преобразуем данные в DataFrame
    raw_df = pd.DataFrame(raw_data, columns=raw_data_columns)

    # Создаём целевую папку временного хранилища, если она не существует
    if not os.path.exists(temp_folder_path):
        print("Создаём папку", temp_folder_path)
        os.makedirs(temp_folder_path)

    # Создаём / очищаем файл временного хранилища
    open(raw_data_csv_path, 'w').close()

    # Записываем данные в файл
    raw_df.to_csv(raw_data_csv_path, sep=';', header=False, index=False)


def load_raw_data_to_dwh_temp():
    """
    Функция для перегрузки сырых данных из файлового хранилища во временную таблицу DWH
    В процессе перегрузки вычисляется хэш строк и добавляется уникальный id каждой строке
    :return: None
    """

    # Выгружаем данные из файла в DataFrame
    try:
        raw_df = pd.read_csv(raw_data_csv_path, sep=';', names=raw_data_columns)
    except FileNotFoundError:
        raise FileNotFoundError('Временный файл с сырыми данными не обнаружен, необходимо повторить выгрузку')

    # Вычисляем хэш
    raw_df['row_hash'] = raw_df.apply(lambda x: hash(tuple(x)), axis=1)

    # Переименовываем столбцы в соответствии с ранее заданным маппингом
    raw_df = raw_df.rename(columns=mapping_dict)

    # Пытаемся установить соединение с БД
    with PGConnector(dbname=config.dwh_dbname,
                     dbuser=config.dwh_dbuser,
                     dbpass=config.dwh_dbpass,
                     dbhost=config.dwh_dbhost,
                     dbport=config.dwh_dbport) as pg_connector:

        # Если соединение с БД установлено переносим данные во временную таблицу DWH
        if pg_connector.conn:
            column_names = ','.join(list(raw_df.columns))
            # Колонка "id" во временной таблице DWH имеет тип bigserial, что позволяет заполнять её автоматически, не задавая в raw_df
            query = f'INSERT into {config.temp_dwh_schema}.{config.temp_dwh_table} ({column_names}) VALUES %s'
            values = [tuple(row) for row in raw_df.to_numpy()]
            pg_connector.input_values(query, values)
        else:
            raise ConnectionError('Нет соединения с БД')


def transform_data_to_dwh_target():
    """
    Функция для перегрузки данных из временной таблицы DWH в целевую с дедубликацией по хэшу
    :return: None
    """

    # Формируем SQL-запрос на перегрузку данных с дедубликацией
    query = f'INSERT INTO {config.target_dwh_schema}.{config.target_dwh_table} ({", ".join(target_data_columns)}) '\
            f'SELECT {", ".join(target_data_columns)} FROM {config.temp_dwh_schema}.{config.temp_dwh_table} AS temp ' \
            f'WHERE temp.row_hash NOT IN (SELECT row_hash FROM {config.target_dwh_schema}.{config.target_dwh_table})'

    with PGConnector(dbname=config.dwh_dbname,
                     dbuser=config.dwh_dbuser,
                     dbpass=config.dwh_dbpass,
                     dbhost=config.dwh_dbhost,
                     dbport=config.dwh_dbport) as pg_connector:

        # Если соедниение с БД установлено выполняем запрос
        if pg_connector.conn:
            pg_connector.input(query)
        else:
            raise ConnectionError('Нет соединения с БД')


with DAG('sky_eng',
         default_args=default_args,
         description='Sky Eng Data Engineer Test Task',
         schedule_interval=config.schedule_interval,
         start_date=datetime.datetime.now(),
         tags=['sky_eng']) as dag:

    extract_raw_data = PythonOperator(
        task_id='extract_raw_data',
        python_callable=extract_from_postgres_to_csv
    )

    load_to_dwh_temp = PythonOperator(
        task_id='load_to_dwh_temp',
        python_callable=load_raw_data_to_dwh_temp
    )

    load_to_dwh_target = PythonOperator(
        task_id='load_to_dwh_target',
        python_callable=transform_data_to_dwh_target,
    )

    extract_raw_data >> load_to_dwh_temp >> load_to_dwh_target

    extract_raw_data.doc_md = "Выгрузка данных из источника (СУБД) во временное файловое хранилище (local, s3, mini, etc) в csv формате."

    load_to_dwh_temp.doc_md = "Выгрузка данных из файлового хранилища во временную таблицу dwh (postgres)."

    load_to_dwh_target.doc_md = "Перемещение данных c дедубликацией из временной таблицы в целевую."
