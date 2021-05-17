import datetime
import psycopg2
from configuration import Config


class RemoteServer:
    """
    A class to represent the anyblock.net postgreSQL client.
    It fetches data from the anyblock.net sql server.
    It fetches some chunks of data in the caching stage.

    ATTRIBUTES
    ----------
    conn: Object
        psycopg2 connection
    table: str
        ethereum mainnet balance table name
    begin: datetime
        the time from when it starts to fetch
    """

    def __init__(self):
        """
        Constructor
        Connects to the remote postgre SQL server and loads the lastly cached time
        """
        self.conn = None
        self.table = Config.DB_NAME

        # This is the ethereum's birthday
        self.begin = datetime.datetime(2015, 7, 30, 15, 0, 0)

        # connect to the server
        try:
            self.conn = psycopg2.connect(
                host=Config.ANY_SERVER,
                port=Config.ANY_PORT,
                user=Config.ANY_USER,
                password=Config.ANY_PASSWORD,
                dbname=Config.ANY_DB
            )

        except psycopg2.Error as e:
            print(e)

        # if the status file has the last cached time,
        # set the self.begin to it.
        self.load_time()

    def fetch_data(self, begin, end):
        """
        Fetch data from the server between begin and end
        :param begin: datetime
            specifies from when to fetch
        :param end: datetime
            specifies till when to fetch
        :return: 2D array
        """

        sql = "SELECT block_number, address, balance, timestamp FROM {} WHERE timestamp BETWEEN %s AND %s"\
            .format(self.table)

        try:
            cursor = self.conn.cursor()
            cursor.execute(sql, (begin, end))
            return cursor.fetchall()
        except psycopg2.Error as e:
            print(e)

        return False

    def load_time(self):
        """
        load the last caching time from the log file
        :return: void
        """

        date = None
        try:
            with open(Config.STATUS_CACHE, "r") as f:
                date = datetime.datetime.fromisoformat(f.read())
        except (IOError, ValueError) as e:
            print(e)

        if date:
            self.begin = date

    def save_time(self):
        """
        save the cached time in the log file
        :return: boolean
        """

        try:
            with open(Config.STATUS_CACHE, "w") as f:
                f.write(self.begin.isoformat())
            return True
        except IOError as e:
            print(e)

        return False

    def auto_fetch(self):
        """
        fetch data since the last caching for time gap
        :return: 2D array when success, or false
        """

        # calculate the end time
        end = self.begin + datetime.timedelta(seconds=Config.TIME_GAP)

        # try fetch data
        data = self.fetch_data(self.begin, end)
        if data is False:
            return False

        # set the begin time as the end for the next fetch
        self.begin = end

        return data
