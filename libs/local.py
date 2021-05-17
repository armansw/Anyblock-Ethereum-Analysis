import datetime
import sqlite3

import psycopg2

from configuration import Config
import time


class LocalCache:
    """
    A class that represents the local caching sqlite interface.
    In caching stage, LocalCache class is to store the balance data from the anyblock.net
    into separated sqlite dbs according to the date.
    In calculating stage, it can be also used to read chunks of daily cache db.

    ATTRIBUTES
    ----------
    conn: object
        sqlite3 connection object
    table: str
        balance, the table name
    """

    def __init__(self, db_file=""):
        """
        Constructor. connects to the sqlite file
        :param db_file: str
            sqlite db file name. If not specified, use the default value: cache.sqlite
        """

        self.conn = None

        # default DB file name
        self.table = Config.DB_NAME

        # the sqlite db file name
        # if the argument is not set, it is the default name
        db = db_file
        if db == "":
            db = Config.DB_FILE

        # open the db file
        try:
            self.conn = sqlite3.connect(db)
        except sqlite3.Error as e:
            print(e)

    def create_table(self):
        """
        Creates the balance table
        :return:
        """

        sql = "CREATE TABLE IF NOT EXISTS {} (" \
              "block INTEGER NOT NULL," \
              "address text NOT NULL," \
              "balance REAL NOT NULL," \
              "timestamp INTEGER NOT NULL); ".format(self.table)

        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
        except sqlite3.Error as e:
            print(e)
            return False

        return True

    def proc_record(self, data):
        """
        process balance records from the anyblock postgreSQL server
        :param data: array
            2D array that are fetched from the remote server.
            array of [block(int), address(str), balance(double), timestamp(datetime)]
        :return: boolean
        """

        try:
            cursor = self.conn.cursor()

            for row in data:

                # retrieve data from row
                block = int(row[0])
                address = row[1]
                # For local cache, the balance is stored in Szabo unit
                balance = float(row[2]) * Config.UNIT_TRANS
                timestamp = row[3]

                # if balance == 0 remove that record
                if balance == 0:
                    sql = "DELETE FROM {} WHERE address=?".format(self.table)
                    cursor.execute(sql, (address,))

                else:
                    # check if the address already exists
                    sql = "SELECT * FROM {} WHERE address=?".format(self.table)
                    cursor.execute(sql, (address,))

                    if len(cursor.fetchall()) > 0:
                        # exists. update the record
                        sql = "UPDATE {} SET block=?, balance=?, timestamp=? WHERE address=?"\
                            .format(self.table)
                        cursor.execute(sql, (block, balance, time.mktime(timestamp.timetuple()), address))

                    else:
                        # not exists. insert a new one
                        sql = "INSERT INTO {} (block, address, balance, timestamp) VALUES (?, ?, ?, ?)"\
                            .format(self.table)
                        cursor.execute(sql, (block, address, balance, time.mktime(timestamp.timetuple())))

            self.conn.commit()

        except sqlite3.Error as e:
            print(e)
            return False

        return True

    def proc_record_from_local(self, data):
        """
        Process balance records from the local cache db files.
        It is originally designed for merging local cache chunks into one cache file,
        but currently not used.

        :param data: array
            2D array that are fetched from the remote server.
            array of [block(int), address(str), balance(double), timestamp(int)]
        :return: boolean
        """

        try:
            cursor = self.conn.cursor()

            for row in data:
                block = int(row[0])
                address = row[1]
                # For local cache, the balance is stored in Szabo unit
                balance = float(row[2])
                timestamp = row[3]

                # if balance == 0 remove that record
                if balance == 0:
                    sql = "DELETE FROM {} WHERE address=?".format(self.table)
                    cursor.execute(sql, (address,))

                else:
                    # check if the address already exists
                    sql = "SELECT * FROM {} WHERE address=?".format(self.table)
                    cursor.execute(sql, (address,))

                    if len(cursor.fetchall()) > 0:
                        # exists. update the record
                        sql = "UPDATE {} SET block=?, balance=?, timestamp=? WHERE address=?"\
                            .format(self.table)
                        cursor.execute(sql, (block, balance, timestamp, address))

                    else:
                        # not exists. insert a new one
                        sql = "INSERT INTO {} (block, address, balance, timestamp) VALUES (?, ?, ?, ?)"\
                            .format(self.table)
                        cursor.execute(sql, (block, address, balance, timestamp))

            self.conn.commit()

        except sqlite3.Error as e:
            print(e)
            return False

        return True

    def get_all(self):
        """
        get all data from a sqlite chunk file
        :return: array
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM {}".format(self.table))
            return cursor.fetchall()
        except sqlite3.Error as e:
            print(e)

        return False

    def get_record_count(self):
        """
        get the record count in a sqlite chunk file
        :return: int
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM {}".format(self.table))
            return cursor.fetchone()
        except sqlite3.Error as e:
            print(e)

        return False

    def get_chunk(self, offset, count):
        """
        get a part of data from a sqlite chunk file
        :param offset: int
            specifies the pk from where to fetch
        :param count: int
            the number of rows to fetch
        :return: array
        """

        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM {} ORDER BY timestamp OFFSET ? LIMIT ?".format(self.table), (offset, count))
            return cursor.fetchall()
        except sqlite3.Error as e:
            print(e)

        return False

    def close(self):
        if self.conn:
            self.conn.close()


class LocalCalc:
    """
    A class that represents the local database that stores all data for LTH/STH calculation
    In the calculating stage, information flows from the cached chunks into this db.

    ATTRIBUTES
    ----------
    conn: object
        sqlite3 connection object
    """

    def __init__(self):
        """
        Constructor
        """
        self.conn = None

        # open the db
        try:
            self.conn = psycopg2.connect(
                host=Config.LOCAL_SERVER,
                port=Config.LOCAL_PORT,
                user=Config.LOCAL_USER,
                password=Config.LOCAL_PASSWORD,
                dbname=Config.LOCAL_DB
            )
        except psycopg2.Error as e:
            print(e)

    def create_tables(self):
        """
        Creates 3 tables
        :return: boolean
        """

        cursor = self.conn.cursor()

        try:

            """
            logs table
            It is a filtered table from the anyblock.net postgresql balance table.
            It contains only address, balance, and timestamp fields from the balance table.
            When there are several balance changes a day for an address,
            it stores only the last balance information.
            """

            sql = "CREATE TABLE IF NOT EXISTS {} (" \
                  "address TEXT NOT NULL," \
                  "balance DOUBLE PRECISION NOT NULL," \
                  "ts DATE NOT NULL); ".format(Config.TABLE_LOG)
            cursor.execute(sql)

            # create indexes on logs table
            sql = "CREATE INDEX IF NOT EXISTS address_idx ON {} (address)".format(Config.TABLE_LOG)
            cursor.execute(sql)

            sql = "CREATE INDEX IF NOT EXISTS address_time_idx ON {} (address, ts)".format(Config.TABLE_LOG)
            cursor.execute(sql)

            """
            addresses table
            It is the last status of all addresses.
            
            FIELDS
            ------
            address: str
                unique address
            balance: float
                final balance of the address
            wallet: str
                'L' for LTH, 'S' for STH
            """
            sql = "CREATE TABLE IF NOT EXISTS {} (" \
                  "address TEXT NOT NULL," \
                  "balance DOUBLE PRECISION NOT NULL," \
                  "wallet TEXT NOT NULL); ".format(Config.TABLE_ADDRESS)
            cursor.execute(sql)

            # create indexes on addresses table
            sql = "CREATE UNIQUE INDEX IF NOT EXISTS address_idx ON {} (address)".format(Config.TABLE_ADDRESS)
            cursor.execute(sql)

            sql = "CREATE INDEX IF NOT EXISTS wallet_idx ON {} (wallet)".format(Config.TABLE_ADDRESS)
            cursor.execute(sql)

            """
            history table
            It store every day's history of LTH/STH total balance change
            
            FIELDS
            ------
            timestamp: int
                the date of history
            lth: float
                total LTH balance of the day
            sth: float
                total STH balance of the day
            """

            sql = "CREATE TABLE IF NOT EXISTS {} (" \
                  "timestamp DATE NOT NULL," \
                  "lth DOUBLE PRECISION NOT NULL," \
                  "sth DOUBLE PRECISION NOT NULL); ".format(Config.TABLE_HISTORY)
            cursor.execute(sql)

            # create indexes on history table
            sql = "CREATE UNIQUE INDEX IF NOT EXISTS time_idx ON {} (timestamp)".format(Config.TABLE_HISTORY)
            cursor.execute(sql)

            self.conn.commit()

        except psycopg2.Error as e:
            print(e)
            return False

        return True

    def get_history(self):
        """
        Get the LTH/STH history from the history table
        :return: array
        """

        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM {} ORDER BY timestamp".format(Config.TABLE_HISTORY))
            return cursor.fetchall()
        except psycopg2.Error as e:
            print(e)

        return False

    def get_last_history(self):
        """
        Get the last LTH/STH balance data for main pie
        :return: date, lth, sth
        """

        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM {} ORDER BY timestamp DESC LIMIT 1".format(Config.TABLE_HISTORY))
            return cursor.fetchone()
        except psycopg2.Error as e:
            print(e)

        return False

    def add_history(self, timestamp, lth, sth):
        """
        Add a new day's LTH, STH balance.
        If the day is already exists, update it.

        :param timestamp: int
            day of history
        :param lth: float
            total balance in LTH wallet
        :param sth: float
            total balance in STH wallet
        :return: boolean
        """

        try:
            cursor = self.conn.cursor()

            # check if the address already exists
            sql = "SELECT * FROM {} WHERE timestamp=%s".format(Config.TABLE_HISTORY)
            cursor.execute(sql, (timestamp,))

            if len(cursor.fetchall()) > 0:
                # exists. update the record
                sql = "UPDATE {} SET lth=%s, sth=%s WHERE timestamp=%s" \
                    .format(Config.TABLE_HISTORY)
                cursor.execute(sql, (lth, sth, timestamp))

            else:
                # not exists. insert a new one
                sql = "INSERT INTO {} (timestamp, lth, sth) VALUES (%s, %s, %s)" \
                    .format(Config.TABLE_HISTORY)
                cursor.execute(sql, (timestamp, lth, sth))

        except psycopg2.Error as e:
            print(e)
            return False

        return True

    def get_lth_sth(self):
        """
        Gets the current LTH and STH balances from the addresses table.
        It is designed to be used when a day's cache chunk was processed
        and then adds the updated LTH/STH balance into history table.
        :return: (lth, sth)
        """

        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT wallet, SUM(balance) FROM {} GROUP BY wallet ORDER BY wallet".
                           format(Config.TABLE_ADDRESS))
            return cursor.fetchall()
        except psycopg2.Error as e:
            print(e)

        return False

    def update_address_wallet(self, address, wallet):
        """
        Update the addresses table.
        Updates the wallet for an already existing address.

        :param address: str
            ethereum address
        :param wallet: str
            'L' for LTH, 'S' for STH
        :return:
        """

        try:
            cursor = self.conn.cursor()

            sql = "UPDATE {} SET wallet=%s WHERE address=%s" \
                .format(Config.TABLE_ADDRESS)
            cursor.execute(sql, (wallet, address))

        except psycopg2.Error as e:
            print(e)
            return False

        return True

    def check_address(self, address, balance):
        """
        Check if the address already exists in the addresses table.
        If not exists, add it with the default 'S' value of wallet.

        :param address: str
            ethereum address
        :param balance: float
            balance of the address
        :return:
        """

        try:
            cursor = self.conn.cursor()

            # check if the address already exists
            sql = "SELECT * FROM {} WHERE address=%s".format(Config.TABLE_ADDRESS)
            cursor.execute(sql, (address,))

            if len(cursor.fetchall()) == 0:
                # not exists. insert a new one
                sql = "INSERT INTO {} (address, balance, wallet) VALUES (%s, %s, %s)" \
                    .format(Config.TABLE_ADDRESS)
                cursor.execute(sql, (address, balance, 'S'))

            else:
                # exists. update the balance
                sql = "UPDATE {} SET balance=%s WHERE address=%s" \
                    .format(Config.TABLE_ADDRESS)
                cursor.execute(sql, (balance, address))

        except psycopg2.Error as e:
            print(e)
            return False

        return True

    def get_address_count(self):
        """
        Return the row count of addresses table
        Returns
        -------
        row count
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM {}".format(Config.TABLE_ADDRESS))
        row = cursor.fetchone()
        return row[0]

    def get_addresses(self, offset, count):
        """
        Return the address list from offset
        Parameters
        ----------
        offset: int
            cursor offset order by address
        count: int
            number of addresses

        Returns
        -------
        array(address), array(balance)
        """

        cursor = self.conn.cursor()
        cursor.execute("SELECT address, balance FROM {} ORDER BY address OFFSET %s LIMIT %s"
                       .format(Config.TABLE_ADDRESS),
                       (offset, count))
        res = cursor.fetchall()
        return [r[0] for r in res], [r[1] for r in res]

    def add_log(self, address, balance, timestamp):
        """
        Add a record from the caches into log table

        :param address: str
        :param balance: float
        :param timestamp: int
        :return: boolean
        """

        try:
            cursor = self.conn.cursor()

            # check if the address already exists
            sql = "SELECT * FROM {} WHERE address=%s AND balance=%s and ts=%s".format(Config.TABLE_LOG)
            cursor.execute(sql, (address, balance, timestamp))

            if len(cursor.fetchall()) > 0:
                # exists. don't need to add
                return True

            sql = "INSERT INTO {} (address, balance, ts) VALUES (%s, %s, %s)".format(Config.TABLE_LOG)
            cursor.execute(sql, (address, balance, timestamp))

        except psycopg2.Error as e:
            print(e)
            return False

        return True

    def add_logs(self, data):
        """
        Add the whole logs from a day
        Parameters
        ----------
        data: array(Union[int, str, float, int])
            fetchall() data

        Returns
        -------
        boolean
        """

        try:
            for row in data:
                # convert fields from row
                address = row[1]
                balance = float(row[2])
                ts = int(row[3])

                # floor the datetime into date
                dt = datetime.datetime.utcfromtimestamp(ts)
                day = dt.date()

                # check and add
                self.check_address(address, balance)
                self.add_log(address, balance, day)

            self.conn.commit()
            return True

        except psycopg2.Error as e:
            print(e)

        return False

    def calculate_lth_sth(self, address, dt, balance):
        """
        Calculate LTH or STH for address
        Parameters
        ----------
        address: str
            address for calculating
        dt: datetime.datetime object
            current date
        balance: float
            current balance
        Returns
        -------
        'L' for LTH, 'S' for STH
        """

        cursor = self.conn.cursor()

        # Stage 1)
        # If there is no records in recent 155 days, it is LTH

        # get time limits
        end_dt = dt + datetime.timedelta(days=-1)
        begin_dt = end_dt + datetime.timedelta(days=-Config.WALLET_THRESHOLD)

        sql = f'SELECT COUNT(*) FROM {Config.TABLE_LOG} WHERE address=%s AND ts BETWEEN %s AND %s'
        cursor.execute(sql, (address, begin_dt, end_dt))
        row = cursor.fetchone()

        if row[0] < 1:
            return 'L'

        # Stage 2)
        # balance < 2 * Weighed_RSM for 6 month, it is LTH
        begin_dt = end_dt + datetime.timedelta(days=-Config.WINDOW_SIZE)

        try:

            # get weight sum
            sql = "SELECT " \
                  "SUM(1-LOG(180, CAST(DATE_PART(CAST(%s AS TEXT), %s::timestamp - ts::timestamp) AS numeric))) " \
                  f"FROM {Config.TABLE_LOG} " \
                  "WHERE address=%s AND ts BETWEEN %s AND %s"

            cursor.execute(sql, ('day', dt, address, begin_dt, end_dt))
            ret = cursor.fetchone()
            weights = float(ret[0])

            # get previous balance level
            sql = "SELECT " \
                  "2 * SQRT(SUM(balance * balance * (1-LOG(180, " \
                  "CAST(DATE_PART(CAST(%s AS TEXT), %s::timestamp - ts::timestamp) " \
                  "AS numeric)))))/%s " \
                  "FROM {} " \
                  "WHERE address=%s AND ts BETWEEN %s AND %s".format(Config.TABLE_LOG)
            cursor.execute(sql, ('day', dt, weights, address, begin_dt, end_dt))
            ret = cursor.fetchone()
            pbl = ret[0]

            if balance <= pbl:
                return 'L'

        except psycopg2.Error as e:
            return 'S'

        return 'S'
