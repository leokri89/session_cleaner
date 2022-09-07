from abc import ABC, abstractmethod

import psycopg2


class Connection(ABC):
    @abstractmethod
    def connect(self):
        return NotImplemented

    @abstractmethod
    def session(self):
        return NotImplemented

    @abstractmethod
    def execute(self):
        return NotImplemented

    @abstractmethod
    def fetchall(self):
        return NotImplemented

    @abstractmethod
    def fetchone(self):
        return NotImplemented


class PostgresConnection(Connection):
    def connect(dbname='test', user='postgres', password='postgres', host='localhost', port='5432'):
        return psycopg2.connect(
            dbname=dbname, user=user, password=password, host=host, port=port
        )

    def session(self):
        return self.cursor()

    def execute(self, sql):
        return self.execute(sql)

    def fetchall(self):
        return self.fetchall()

    def fetchone(self):
        return self.fetchone()
