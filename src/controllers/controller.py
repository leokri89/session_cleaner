import logging
from datetime import datetime, timedelta

import pytz

import repositories.repository as repository
from repositories.databases import PostgresConnection

logging.basicConfig(
    format="%(asctime)s - %(message)s", datefmt="%d-%b-%y %H:%M:%S", level=logging.DEBUG
)


class ProcessSessionCleaner:
    def __init__(self, max_age_hour):
        self.max_datetime = datetime.now(tz=pytz.UTC) - timedelta(hours=max_age_hour)

    def _check_empty_list(self, list):
        if len(list) < 1:
            logging.warning("No sessions to finish")
            exit()

    def execute(self):
        conn = PostgresConnection.connect(
            dbname="postgres",
            user="postgres",
            password="mysecretpassword",
            host="127.0.0.1",
            port="5432",
        )
        cursor = conn.cursor()
        repo = repository.PostgresRepository(cursor)
        user_sessions = repo.get_user_sessions()
        self._check_empty_list(user_sessions)
        ruled_user_sessions = []
        for user_session in user_sessions:
            if user_session.starttime < self.max_datetime:
                ruled_user_sessions.append(user_session)
        self._check_empty_list(ruled_user_sessions)
        repo.del_user_sessions(ruled_user_sessions)
        repo.log_events(ruled_user_sessions)
