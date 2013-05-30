import MySQLdb

in_test = False


def start_test():
    global in_test
    in_test = True


def end_test():
    global in_test
    in_test = False


def _settings_from_section(config, section):
    return dict(host=config.get(section, 'host'),
                user=config.get(section, 'user'),
                passwd=config.get(section, 'passwd'),
                db=config.get(section, 'db'))


def open_conn(config):
    if in_test:
        if not config.has_section('db-test'):
            raise Exception("In test and no db-test config!")
        db = MySQLdb.connect(**_settings_from_section(config, 'db-test'))
    else:
        if not config.has_section('db'):
            raise Exception("Not in test and no db config!")
        db = MySQLdb.connect(**_settings_from_section(config, 'db'))

    return db
