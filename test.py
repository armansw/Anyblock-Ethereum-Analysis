from configuration import Config
import psycopg2
import time
import datetime
from libs.local import LocalCache
from libs.remote import RemotePostgreSQL


def test_remote_connect():
    conn = None

    try:
        # try to connect
        conn = psycopg2.connect(
            host=Config.ANY_SERVER,
            port=Config.ANY_PORT,
            user=Config.ANY_USER,
            password=Config.ANY_PASSWORD,
            dbname=Config.ANY_DB
        )

        # try to select last 10 records
        cursor = conn.cursor()
        query = "SELECT * FROM balance ORDER BY timestamp DESC LIMIT 10"
        cursor.execute(query)

        records = cursor.fetchall()

        for row in records:
            print("B_N=", row[2], type(row[2]))
            print("ADR=", row[3], type(row[3]))
            print("BAL=", row[4], type(row[4]))
            print("TMS=", row[5], type(row[5]))
            print("")

    except psycopg2.Error as e:
        print(e.pgerror)
        exit(1)

    finally:
        if conn:
            conn.close()


def test_timestamp():
    before = datetime.datetime(2015, 7, 30, 15, 0)
    after = datetime.datetime(2015, 7, 30, 16, 0)
    print(before)
    print(after)
    print("delta=", time.mktime(after.timetuple()) - time.mktime(before.timetuple()))


def test_local():
    local = LocalCache()
    local.create_table()

    # insert initial data
    local.proc_record(3, [[1, "0x33333333333", 11223344556677, datetime.datetime(2021, 5, 6, 15, 38, 40)]])
    local.proc_record(4, [[1, "0x44444444444", 22334455, datetime.datetime(2021, 5, 7, 15, 38, 40)]])
    local.proc_record(5, [[1, "0x55555555555", 345, datetime.datetime(2021, 5, 8, 15, 38, 40)]])

    # checking data
    # should not be inserted
    local.proc_record(6, [[1, "0x33333333333", 11223344556677, datetime.datetime(2021, 5, 6, 15, 38, 40)]])

    # should be deleted
    local.proc_record(7, [[1, "0x55555555555", 0, datetime.datetime(2021, 5, 6, 15, 38, 40)]])

    # check that only two records are stored.
    rows = local.get_all()
    for row in rows:
        print(row)

    local.close()


def test_remote_server():
    server = RemotePostgreSQL()
    begin = datetime.datetime(2021, 5, 7, 15, 0, 0)
    end = datetime.datetime(2021, 5, 7, 15, 1, 0)

    # test fetch_data
    data = server.fetch_data(begin, end)
    print("{} records are retrieved.".format(len(data)))
    for row in data:
        print("{} {} {} {}".format(row[0], row[1], row[2], row[3]))

    # test auto_fetch
    data = server.auto_fetch()
    print("{} records are retrieved from {}.".format(len(data), server.begin))

    data = server.auto_fetch()
    print("{} records are retrieved from {}.".format(len(data), server.begin))


if __name__ == "__main__":

    # test_remote_connect()
    # test_timestamp()
    test_local()
    # test_remote_server()
