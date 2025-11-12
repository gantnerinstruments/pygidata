from iniconfig import ParseError
import sqlalchemy
from sqlalchemy import exc
import logging
import pandas as pd

import pymysql

from gi_data.cloudconnect.utils import (
    split_dates,
    get_dates_from_string,
    replace_dates_in_string,
)

pymysql.install_as_MySQLdb()

# Set output level to INFO because default is WARNNG
logging.getLogger().setLevel(logging.INFO)


class MySQLConnect:

    def __init__(
        self,
        db_host: str,
        db_user: str,
        db_port: str,
        db_pass: str,
        db_name: str,
    ) -> None:
        self.connect_to_db(db_host, db_user, db_pass, db_port, db_name)

    def connect_to_db(self, db_host, db_user, db_pass, db_port, db_name):

        # Convert utf8mb4 so mysql < 5.0.2
        connect_string = "mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8".format(
            db_user, db_pass, db_host, db_port, db_name
        )

        try:
            engine = sqlalchemy.create_engine(connect_string, echo=True)

            self.connection = engine.connect()

            # Check wether conn is none or not to determine if connection is successful
            return self.connection
        except Exception as e:
            # call method to visualize failed connection
            print("Connection to ", db_host, " could not be established.", e)

    def query_sql(self, query: str, set_columns: bool = True, *args, **kwargs) -> pd.DataFrame:

        trans = self.connection.begin()

        try:
            res = self.connection.execute(query)

            trans.commit()
            ref_data = res.fetchall()
            df = pd.DataFrame(ref_data)
            # if set_columns:
            # df.columns = res.keys()
            return df

        except exc.SQLAlchemyError:
            logging.info("SQL Query is wrong.")

    def multiple_calls(self, query: str, N: int, *args, **kwargs) -> pd.DataFrame:
        """Multiple query call to avoid timeout error.
        Parse the start and end date, split it into multiple calls
        and concatenate results.
        """
        dates = get_dates_from_string(query)
        if len(dates) != 2:
            raise ParseError("Could not parse out the dates of query - lookup format!")

        # e.g. if N = 2 -> list size is 3
        dates_to_query = split_dates(dates[0], dates[1], N)

        #
        frames = []
        i = 1
        while i < len(dates_to_query):
            temp_query = replace_dates_in_string(query, [dates_to_query[i - 1], dates_to_query[i]])
            logging.info(f"Querying from {dates_to_query[i - 1]} to {dates_to_query[i]}")

            frames.append(self.query_sql(temp_query))
            i += 1

        return pd.concat(frames, ignore_index=True)
