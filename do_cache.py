from configuration import Config
from libs.local import LocalCache
from libs.remote import RemoteServer
import datetime
import signal

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


def main():

    # connects to the anyblock.net sql server
    remote = RemoteServer()
    if remote.conn is None:
        print("Cannot connect to the server. Try again later.")
        return

    # register the SIGKILL (Ctrl + C) handler
    signal.signal(signal.SIGINT, signal_handler)

    # a flag to check the date change
    current_day = 0

    # local sqlite file interface
    local = None

    while True:
        begin = remote.begin

        year = begin.year
        month = begin.month
        day = begin.day

        if current_day != day:
            # close the sqlite file for yesterday
            if local is not None:
                local.close()

            # open today's new sqlite file
            db_file = "dbs/cache-{}-{}-{}.sqlite".format(year, month, day)
            local = LocalCache(db_file)
            local.create_table()

        # load data from the server
        data = remote.auto_fetch()
        if data is False:
            print("Getting data failed from {} for {} seconds.".format(remote.begin, Config.TIME_GAP))
            break
        else:
            print("{} records are retrieved since {} for {} seconds.".format(len(data), remote.begin, Config.TIME_GAP),
                  end='')

        # store the data
        ret = local.proc_record(data)

        if ret is False:
            print("  Processing data failed.")
            break
        else:
            print("  Stored.")

        # save the time with when it begins for the next time
        remote.save_time()

        # check if all data was cached
        today = datetime.date.today()
        if today == remote.begin.date():
            print("All data were cached until yesterday.")
            break

        # if SIGKILL has been received
        if terminate:
            print("interrupted")
            break

    print("Bye")


if __name__ == "__main__":
    main()
