import MySQLdb

in_test = False


def start_test():
    global in_test
    in_test = True


def end_test():
    global in_test
    in_test = False


def open_conn(db_config):
    if in_test:
        db = MySQLdb.connect(host='localhost',
                             user='root',
                             passwd='root',
                             db='strings_queue_test')
    else:
        db = MySQLdb.connect(**db_config)

    return db
