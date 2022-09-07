import logging
from abc import ABC, abstractmethod
from datetime import datetime

from models.sessions import Session


class Repository(ABC):
    @abstractmethod
    def check_connection_status(self, pid):
        return NotImplemented

    @abstractmethod
    def get_user_sessions(self):
        return NotImplemented

    @abstractmethod
    def del_user_sessions(self, user_sessions):
        return NotImplemented

    @abstractmethod
    def log_events(self, user_sessions):
        return NotImplemented


class FakeRepository(Repository):
    def __init__(self, session):
        self.session = session

    def check_connection_status(self, pid):
        return super().check_connection_status(pid)

    def get_user_sessions(self):
        logging.info(f"Getting sessions")
        user_sessions = [
            Session(1, "user1", datetime(2022, 1, 1, 1, 1, 1), "Live"),
            Session(2, "user2", datetime(2022, 1, 1, 1, 1, 1), "Live"),
            Session(3, "user3", datetime(2022, 1, 1, 1, 1, 1), "Live"),
        ]
        logging.info(f"{len(user_sessions)} sessions returned.")
        return user_sessions

    def del_user_sessions(self, user_sessions):
        for idx, user_session in enumerate(user_sessions):
            try:
                logging.info(f"Killing Session: {user_session.pid}")
                user_sessions[idx].status = "Session finished"
            except:
                logging.warning(f"Erro to cancel pid {user_session.pid}")
                user_sessions[idx].status = "Fail to finish session"
        return user_sessions

    def log_events(self, user_sessions):
        success_canceled = 0
        failed_canceled = 0
        for user_session in user_sessions:
            if user_session.status == "Session finished":
                success_canceled += 1
            else:
                failed_canceled += 1
        logging.info(f"Logging {success_canceled} success canceled sessions")
        logging.info(f"Logging {failed_canceled} failed canceled sessions")
        return "OK"


class PostgresRepository(Repository):
    def __init__(self, session):
        self.session = session

    def check_connection_status(self, pid):
        try:
            self.session.execute(
                f"SELECT count(1) r from pg_stat_activity where pid={pid}"
            )
            if self.session.fetchone()[0] > 0:
                return "Fail to finish session"
            return "Session finished"
        except:
            return "Fail to confirm status"

    def get_user_sessions(self):
        sql = """select pid, usename, backend_start starttime 
        from pg_stat_activity 
        where usename is not null and
            pid <> pg_backend_pid() and
            usename = 'user1'
        order by pid"""
        self.session.execute(sql)
        user_sessions = []
        for user_session in self.session.fetchall():
            user_sessions.append(
                Session(
                    pid=user_session[0],
                    username=user_session[1],
                    starttime=user_session[2],
                    status="Live",
                )
            )
        return user_sessions

    def del_user_sessions(self, user_sessions):
        for idx, user_session in enumerate(user_sessions):
            try:
                logging.info(f"Killing Session: {user_session.pid}")
                self.session.execute(f"SELECT pg_terminate_backend({user_session.pid})")
                user_sessions[idx].status = self.check_connection_status(
                    user_session.pid
                )
            except:
                logging.warning(f"Erro to cancel pid {user_session.pid}")
                user_sessions[idx].status = "Fail to finish session"

    def log_events(self, user_sessions):
        success_canceled = 0
        failed_canceled = 0
        for user_session in user_sessions:
            if user_session.status == "Session finished":
                success_canceled += 1
            else:
                failed_canceled += 1
        logging.info(
            f"Success canceled sessions: {success_canceled}, failed canceled sessions: {failed_canceled}"
        )
