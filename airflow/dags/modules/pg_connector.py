import psycopg2 as pg
from psycopg2 import extras


class PGConnector:
    """
    Класс реализует интерфейс взаимодействия с БД Postgres, где располагается как источник данных, так и целевые таблицы
    """
    def __init__(self, dbname, dbuser, dbpass, dbhost, dbport):
        """
        Класс в момент инициализации пытается устанавливить соединение с БД.
        На вход принимает основные параметры подключения
        На выходе выдает объект psycopg2.connect или None, если соединение не установлено
        :param dbname: имя БД
        :param dbuser: пользователь БД
        :param dbpass: пароль к БД
        :param dbhost: имя хоста БД
        :param dbport: порт БД
        """
        try:
            self.conn = pg.connect(
                database=dbname,
                user=dbuser,
                password=dbpass,
                host=dbhost,
                port=dbport
            )
        except Exception as e:
            print('Нет соединения с БД:')
            print(e)
            self.conn = None

    def __enter__(self):
        """
        Точка входа для обератора with
        :return:
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Точка выхода для оператора with
        :param exc_type:
        :param exc_val:
        :param exc_tb:
        :return:
        """
        self.close()

    def select(self, query: str):
        """
        Обёртка для запросов типа SELECT
        :param query: запрос к БД в виде строки
        :return: результат (кортеж кортежей)
        """
        if self.conn:
            with self.conn.cursor() as cur:
                cur.execute(query)
                return cur.fetchall()
        else:
            print('No DB connection')
            return None

    def input(self, query: str):
        """
        Обёртка для запросов типа INSERT (и UPDATE)
        :param query: запрос к БД в виде строки
        :return:
        """
        if self.conn:
            with self.conn.cursor() as cur:
                cur.execute(query)
                self.conn.commit()
        else:
            print('No DB connection')

    def input_values(self, query: str, values: tuple):
        """
        Обёртка для пакетной вставки данных в базу, использует метод psycopg.extras.execute_values()
        :param query: запрос к БД в виде строки
        :param values: вставляемые значения (кортеж кортежей)
        :return:
        """
        if self.conn:
            with self.conn.cursor() as cur:
                extras.execute_values(cur, query, values)
            self.conn.commit()
        else:
            print('No DB connection')

    """
    Метод больше не используется, оставлен для истории
    def load_from_file(self, fileobj, target_table):
        if self.conn:
            with self.conn.cursor() as cur:
                cur.copy_from(fileobj, target_table, sep=';')
                self.conn.commit()
        else:
            print('No DB connection')
            return None
    """

    def close(self):
        """
        Метод, закрывающий соединение с БД
        :return: None
        """
        if self.conn:
            self.conn.close()
