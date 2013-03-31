"""
Module for processing requests to the stack queue.
"""
from collections import namedtuple

from stackconnector import db


JobInfo = namedtuple('JobInfo', 'id http_method url body')


# This can come from config if we like.
DEFAULT_POOL_SIZE = 5


def _find_job(curs, job_id):
    # TODO actually find the job info in the table where pending =
    # True
    return JobInfo(id=None,
                   http_method=None,
                   url=None,
                   body=None)


class JobResult(object):
    """
    Represents the result of attempting to call a single queued job.
    """

    def is_success(self):
        """
        True if the job was successful.
        """
        # TODO make this actually, y'know, return whether the job
        # succeeded or not.
        return False


def _call_job(job_info):
    # open up an http connection, hit the endpoint, with a timeout.
    # return the JobResult object that we get from parsing the json
    # returned.
    pass


def _mark_done(curs, job_id):
    """
    Mark the job as done.
    """
    pass


def _log_success(curs, job_id):
    """
    Log a success for job_id.
    """
    pass


def _log_failure(curs, job_id):
    """
    Log a failure for job_id.
    """
    pass


def process_one(job_id):
    conn = None
    curs = None
    try:
        conn = db.open_conn()
        curs = conn.cursor()
        if not _lock_job(curs, job_id):
            # return something indicating we couldn't get a lock on
            # the job.
            return None
        job_info = _find_job(curs, job_id)
        result = _call_job(job_info)
        if result.is_success():
            _mark_done(curs, job_id)
            _log_success(curs, job_id)
        else:
            _log_failure(curs, job_id)
    finally:
        # This will automatically release the lock, if one was
        # acquired.
        if curs:
            curs.close()
        if conn:
            conn.close()


def _find_all_pending(conn):
    # TODO: find all the pending job ids and return them, don't worry
    # about locking, which will be handled in process_one.  Some
    # mechanism in the db to show jobs that have been retried too many
    # times and should be considered dead.
    return []


def process_all(conn, pool_size=DEFAULT_POOL_SIZE):
    """
    Process all pending jobs in the queue, running up to @pool_size
    concurrently.
    """
    pool = Pool(DEFAULT_POOL_SIZE)
    pending_job_ids = _find_all_pending(conn)
    results = pool.map_async(process_one)
