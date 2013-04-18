#!/usr/bin/env python

"""
Module for processing requests to the stack queue.
"""

from collections import namedtuple
from ConfigParser import SafeConfigParser
import logging
from multiprocessing import Pool
from optparse import OptionParser
import requests
import sys
from textwrap import dedent

from jobqueue import db


logging.basicConfig(level=logging.INFO,
                    stream=sys.stderr,
                    format="%(asctime)s  %(message)s")


# Result codes for the db.
SUCCESS = 0
PERMANENT_FAILURE = 1
TEMPORARY_FAILURE = 2


LOCK_TIMEOUT_SECS = 10


JobInfo = namedtuple('JobInfo', ['id', 'http_method', 'url', 'body',
                                 'timeout_secs', 'last_started_at', 'result_code',
                                 'remaining_retries', 'retry_delay_secs'])


# This can come from config if we like.
DEFAULT_POOL_SIZE = 5


def _log_to_db(curs, job_id, msg):
    msg = "[JOBID %s] %s" % (job_id, msg)
    logging.info(msg)
    insert = """
             INSERT INTO queued_job_log
               (job_id, msg)
             VALUES
               (%s, %s)
             """
    curs.execute(insert, (job_id, msg))


def _find_job(curs, job_id):
    select = """
             SELECT id, http_method, url, body, timeout_secs,
                        last_started_at, result_code, remaining_retries,
                        retry_delay_secs
             FROM queued_job
             WHERE id = %s
             """
    curs.execute(select, (job_id,))
    row = curs.fetchone()
    if not row:
        return None
    else:
        return JobInfo(id=row[0],
                       http_method=row[1],
                       url=row[2],
                       body=row[3],
                       timeout_secs=row[4],
                       last_started_at=row[5],
                       result_code=row[6],
                       remaining_retries=row[7],
                       retry_delay_secs=row[8])


class JobResult(object):
    """
    Represents the result of attempting to call a single queued job.
    """

    def __init__(self, is_timeout=False, status_code=None, text=None,
                 new_retry_delay_secs=None):
        self.is_timeout = is_timeout
        self.status_code = status_code
        self.text = text
        self.new_retry_delay_secs = new_retry_delay_secs

    def is_success(self):
        """
        True if the job was successful.
        """
        return self.status_code == 200

    def is_permanent_failure(self):
        """
        True if the job was a permanent failure.

        A permanent failure:

        - was not a timeout and
        - was not an error code 503
        """
        return not self.is_timeout and self.status_code != 503


def _mark_started(curs, job_id):
    mark = """
           UPDATE queued_job
             SET last_started_at = NOW()
             WHERE id = %s
           """
    curs.execute(mark, (job_id,))


def _call_job(curs, job_info):
    # open up an http connection, hit the endpoint, with a timeout.
    # return the JobResult object that we get from parsing the json
    # returned.
    new_retry_delay_secs = None
    text = None

    _mark_started(curs, job_info.id)
    _log_to_db(curs, job_info.id,
               "Calling job with method %s on url %s (timeout %s)" %
               (job_info.http_method,
                job_info.url,
                job_info.timeout_secs))

    try:
        resp = requests.request(job_info.http_method,
                                job_info.url,
                                data=job_info.body,
                                timeout=float(job_info.timeout_secs))
        text = resp.text
        # if the request has temporarily failed, and asked for a new
        # retry delay, respect it
        new_retry_delay_secs = resp.headers['x-bitlancer-retry-delay-secs']
    except requests.Timeout:
        return JobResult(is_timeout=True)

    return JobResult(status_code=resp.status_code,
                     text=resp.text,
                     new_retry_delay_secs=new_retry_delay_secs)


def _set_result_code(curs, job_id, result_code):
    mark_done = """
                UPDATE queued_job
                  SET result_code = %s
                  WHERE id = %s
                """
    curs.execute(mark_done, (result_code, job_id))


def _mark_finished(curs, job_id):
    mark = """
           UPDATE queued_job
             SET last_finished_at = NOW()
             WHERE id = %s
           """
    curs.execute(mark, (job_id,))


def _log_success(curs, job_id, result):
    """
    Log a success for job_id.
    """
    _mark_finished(curs, job_id)
    _set_result_code(curs, job_id, SUCCESS)
    _log_to_db(curs, job_id, "Job succeeded: %s" % result.text)


def _decrement_retries(curs, job_id):
    decrement = """
                UPDATE queued_job
                  SET remaining_retries = remaining_retries - 1
                  WHERE id = %s
                """
    curs.execute(decrement, (job_id,))


def _log_failure(curs, job_id, result):
    """
    Log a failure for job_id.
    """
    msg = "Job failed"
    if result.is_permanent_failure():
        result_code = PERMANENT_FAILURE
        msg += (" permanently: %s"  % result.text)
    elif result.is_timeout:
        result_code = TEMPORARY_FAILURE
        msg += " due to timeout"
    else:
        result_code = TEMPORARY_FAILURE
        msg += (" temporarily: %s"  % result.text)
    _log_to_db(curs, job_id, msg)
    _set_result_code(curs, job_id, result_code)
    _decrement_retries(curs, job_id)
    _mark_finished(curs, job_id)


def _fmt_lock_id(job_id):
    return "lock_job_%s" % job_id


def _lock_job(curs, job_id):
    acquire_lock = """
                   SELECT GET_LOCK(%s, %s)
                   """
    curs.execute(acquire_lock, (_fmt_lock_id(job_id), LOCK_TIMEOUT_SECS))
    return curs.fetchone()[0] == 1


def _is_workable(job_info):
    return ((job_info.result_code is None or
             job_info.result_code == TEMPORARY_FAILURE) and
            job_info.remaining_retries > 0)


def process_one(job_id_and_db_config):
    job_id, db_config = job_id_and_db_config

    conn = None
    curs = None

    try:
        conn = db.open_conn(db_config)
        curs = conn.cursor()
        if not _lock_job(curs, job_id):
            _log_to_db(curs, job_id, "Could not acquire lock on job")
            return None
        job_info = _find_job(curs, job_id)
        if not job_info:
            _log_to_db(curs, job_id, "Could not find job")
            return None
        if not _is_workable(job_info):
            _log_to_db(curs, job_id, "Job not workable, skipping")
            return None
        result = _call_job(curs, job_info)
        _maybe_update_retry(curs, job_id, result)
        if result.is_success():
            _log_success(curs, job_id, result)
        else:
            _log_failure(curs, job_id, result)
        return result
    finally:
        # This will automatically release the lock, if one was
        # acquired.
        if curs:
            curs.close()
        if conn:
            conn.close()


def _maybe_update_retry(curs, job_id, result):
    if result.new_retry_delay_secs is not None:

        curs.execute("""
                     UPDATE queued_job
                       SET retry_delay_secs = %s
                       WHERE id = %s
                     """,
                     (result.new_retry_delay_secs, job_id))


def _find_all_pending(curs):
    select = """
             SELECT id
             FROM queued_job
               WHERE
                  (result_code IS NULL OR result_code = %s)
                AND
                  remaining_retries > 0
                AND
                  (last_finished_at IS NULL OR
                   DATE_ADD(last_finished_at,
                            INTERVAL retry_delay_secs SECOND) <= NOW())
             """
    curs.execute(select, (TEMPORARY_FAILURE,))
    rows = curs.fetchall()
    return [r[0] for r in rows]


def process_all(pool, db_config):
    logging.info("Processing all...")

    conn = None
    curs = None

    try:
        logging.info("Opening connection to db...")
        conn = db.open_conn(db_config)
        curs = conn.cursor()
        logging.info("Done opening db connection.")
        # we don't worry about race conditions on starting jobs, since
        # the locking in the single processor will make sure that only
        # one job at a time is actually processing.
        logging.info("Finding pending jobs...")
        pending_job_ids = _find_all_pending(curs)
        logging.info("Done, found %d pending jobs: %s.",
                     len(pending_job_ids),
                     pending_job_ids)
        return pool.map_async(process_one, [(job_id, db_config)
                                            for job_id
                                            in pending_job_ids])
    finally:
        if curs:
            curs.close()
        if conn:
            conn.close()
        logging.info("Done processing all.")


def _parse_db_config(db_conf_fname):
    config_parser = SafeConfigParser()

    with open(db_conf_fname, 'rb') as fil:
        config_parser.readfp(fil)

    return config_parser


def process_with_pool(num_procs, db_config):
    logging.info("Initializing pool of size %d", num_procs)
    pool = Pool(num_procs)
    result = process_all(pool, db_config)
    return result.get()


def main(num_procs, db_conf_fname):
    logging.info("Reading database config from %s", db_conf_fname)
    db_config = _parse_db_config(db_conf_fname)
    return process_with_pool(num_procs, db_config)


if __name__ == '__main__':
    parser = OptionParser(usage=dedent("""\
                                       [options] conf_file
                                       -h or --help for help.

                                       Processes any workable queued jobs and then quits.

                                       conf_file should be a path to a ini-like file containing:

                                       [db]
                                       host: <db_host>
                                       user: <db_user>
                                       passwd: <db_passwd>
                                       db: <db_name>
                                       """))
    parser.add_option("-p", "--procs",
                      type="int", default=DEFAULT_POOL_SIZE,
                      dest="num_procs",
                      help="Number of processes to run (default %s)" % DEFAULT_POOL_SIZE)

    (opts, args) = parser.parse_args()

    if len(args) != 1:
        parser.error("Must pass exactly one conf file.")

    main(num_procs=opts.num_procs,
         db_conf_fname=args[0])

