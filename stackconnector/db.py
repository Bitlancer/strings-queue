import MySQLdb

in_test = False


def start_test():
    global in_test
    in_test = True


def end_test():
    global in_test
    in_test = False


def open_conn():
    if in_test:
        db = MySQLdb.connect(host='localhost',
                             user='root',
                             passwd='root',
                             db='stringstest')
    else:
        # TODO: add ENV sensor here for db creds / namep
        db = MySQLdb.connect(host='localhost',
                             user='root',
                             passwd='root',
                             db='stringsdev')
    return db
