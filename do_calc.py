from configuration import Config
from libs.local import LocalCache, LocalCalc
import datetime
import signal
import sys
import os

# terminate flag
terminate = False


# noinspection PyUnusedLocal
def signal_handler(signum, frame):
    """
    Handle the SIGKILL(Ctrl+C) signal
    :param signum: not used
    :param frame: not used
    :return: void
    """

    global terminate

    # set the terminate flag
    terminate = True


def load_date():
    """
    Load last calculated date from file
    :return: datetime.date object
    """

    # the first date to be calc(ethereum's birthday)
    birth = "2015-07-30"

    contents = ""

    try:
        with open(Config.STATUS_CALC, "r") as f:
            contents = f.read()
    except IOError as e:
        print(e)

    if contents == "":
        contents = birth

    return datetime.date.fromisoformat(contents)


def save_date(date):
    """
    Save last calculated date to file

    :param date: datetime.date object
        the date to save
    :return: boolean
    """
    try:
        with open(Config.STATUS_CALC, "w") as f:
            f.write(date.isoformat())

        return True

    except IOError as e:
        print(e)
        return False


def draw_progress_bar(dt_string, percent, bar_len=20):
    sys.stdout.write("\r")
    progress = ""
    for i in range(bar_len):
        if i < int(bar_len * percent):
            progress += "="
        else:
            progress += " "
    sys.stdout.write("Calculating for %s: [ %s ] %.2f%%" % (dt_string, progress, percent * 100))
    sys.stdout.flush()


def main():

    # connect to the main db
    calculator = LocalCalc()
    if calculator.conn is None:
        print("Cannot open main sqlite file.")
        return

    # create tables
    calculator.create_tables()

    # register the SIGKILL (Ctrl + C) handler
    signal.signal(signal.SIGINT, signal_handler)

    dt = load_date()

    while True:
        # compose the cache db file name
        cache_file = "dbs/cache-{}-{}-{}.sqlite".format(dt.year, dt.month, dt.day)

        # check if the db file exists
        if not os.path.isfile(cache_file):
            print("cache db file does not exist: {}\nQuit.".format(cache_file))
            break

        # open the cache db
        cache = LocalCache(cache_file)

        date_string = dt.isoformat()

        # adding daily logs into calculator
        print("Calculating for {}: merging data...".format(date_string), end='')
        data = cache.get_all()
        if data is False:
            print("{} file was corrupted.".format(cache_file))
            break

        calculator.add_logs(data)

        # calculating lth/sth for every address
        count = calculator.get_address_count()
        offset = 0

        while offset < count:

            addresses, balances = calculator.get_addresses(offset, Config.CALC_CHUNK_SIZE)

            for i, address in enumerate(addresses):
                # get wallet for an address
                wallet = calculator.calculate_lth_sth(address, dt, balances[i])

                # set wallet for an address
                calculator.update_address_wallet(address, wallet)

                # print progress
                percent = (offset + i) / count
                draw_progress_bar(date_string, percent)

            # next chunk
            offset = offset + Config.CALC_CHUNK_SIZE

        # calculate LTH and STH balance
        lth = 0
        sth = 0
        rows = calculator.get_lth_sth()
        for row in rows:
            if row[0] == 'S':
                sth = row[1]
            if row[0] == 'L':
                lth = row[1]

        # store the lth/sth balances into history
        calculator.add_history(dt, lth * Config.UNIT_CALC, sth * Config.UNIT_CALC)

        # commit all changes
        calculator.conn.commit()
        print("\r{} finished.                                                                   "
              .format(date_string))

        # save the next date
        dt = dt + datetime.timedelta(days=1)
        save_date(dt)

        # check if date is today, finish it
        today = datetime.date.today()
        if today == dt:
            print("All calculation finished.")
            break

        if terminate:
            print("interrupted")
            break

    print("Bye")


if __name__ == "__main__":
    main()
