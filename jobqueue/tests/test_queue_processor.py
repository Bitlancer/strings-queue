"""
Test the queue processor.
"""

from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
from ConfigParser import SafeConfigParser
import os
import threading
import time
import unittest

from nose.tools import ok_, eq_, timed

from jobqueue import db, queue_processor


def _make_handler_func(resp_code, method):
    def _handler(self):
        self.send_response(resp_code)
        self.send_header('Content-type','text/text')
        self.end_headers()
        self.wfile.write("%s %d" % (method, resp_code))
    return _handler


def _nolog(*args, **kwargs):
    pass


def _make_handler_class(name, resp_code, do_GET=None, do_POST=None):
    if do_GET is None:
        do_GET = _make_handler_func(resp_code, "GET")
    if do_POST is None:
        do_POST = _make_handler_func(resp_code, "POST")
    return type(name, (BaseHTTPRequestHandler, object),
                dict(do_GET=do_GET,
                     do_POST=do_POST,
                     log_message=_nolog))


def _read_default_db_ini():
    config_parser = SafeConfigParser()

    with open(os.path.join(os.path.dirname(__file__),
                           '..',
                           '..',
                           'db.ini'),
              'rb') as fil:
        config_parser.readfp(fil)

    return config_parser


port = 19898


class TestQueueProcessor(unittest.TestCase):

    def setUp(self):
        db.start_test()
        self.conn = db.open_conn(_read_default_db_ini())
        self._clean_all_tables()
        self.server = None
        self.thread = None

    def tearDown(self):
        if self.server:
            self.server.shutdown()
            self.thread.join()
        self._clean_all_tables()
        global port
        port += 1
        db.end_test()
        self.conn = None

    def _start_server(self, request_handler_class):
        global port
        self.server = HTTPServer(('127.0.0.1', port), request_handler_class)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()

    def _clean_all_tables(self):
        curs = self.conn.cursor()
        for table in ['queued_job', 'queued_job_log']:
            curs.execute("TRUNCATE TABLE %s" % table)

    def _do_test_job_simple(self, resp_code, result_code, template_str):
        job_id = self._queue_job('get', '/test')
        self._start_server(_make_handler_class('Handle%d' % resp_code,
                                               resp_code))
        queue_processor.process_with_pool(1, _read_default_db_ini())
        self._assert_done(job_id, result_code,
                          template_str % (dict(job_id=job_id,
                                               resp_code=resp_code)))

    # help protect against deadlock
    @timed(10)
    def test_simple_endpoint(self):
        self._do_test_job_simple(
            200,
            queue_processor.SUCCESS,
            "[JOBID %(job_id)s] Job succeeded: GET %(resp_code)s")

    # help protect against deadlock
    @timed(10)
    def test_temporary_failure(self):
        self._do_test_job_simple(
            503,
            queue_processor.TEMPORARY_FAILURE,
            "[JOBID %(job_id)s] Job failed temporarily: GET %(resp_code)s")

    # help protect against deadlock
    @timed(10)
    def test_permanent_failure(self):
        self._do_test_job_simple(
            500,
            queue_processor.PERMANENT_FAILURE,
            "[JOBID %(job_id)s] Job failed permanently: GET %(resp_code)s")

    # help protect against deadlock
    @timed(10)
    def test_body_received_on_post(self):
        job_id = self._queue_job('post', '/test', body="this is a test body")

        def _never_called(other_self):
            ok_(False)

        body_seen = []

        def _do_post(other_self):
            other_self.send_response(200)
            other_self.send_header('Content-type','text/text')
            other_self.end_headers()
            other_self.wfile.write("POST 200")
            content_len = int(other_self.headers.getheader('content-length'))
            body_seen.append(other_self.rfile.read(content_len))

        self._start_server(_make_handler_class('CheckBody',
                                               200,
                                               do_GET=_never_called,
                                               do_POST=_do_post))
        queue_processor.process_with_pool(1, _read_default_db_ini())
        self._assert_done(job_id, queue_processor.SUCCESS,
                          "[JOBID %s] Job succeeded: POST 200" % job_id)

        eq_(1, len(body_seen))
        eq_("this is a test body", body_seen[0])

    # help protect against deadlock
    @timed(10)
    def test_retries(self):
        job_id = self._queue_job('get', '/test', remaining_retries=1)
        self._start_server(_make_handler_class('TestRetriesHandler', 503))
        queue_processor.process_with_pool(1, _read_default_db_ini())
        self._assert_done(
            job_id,
            queue_processor.TEMPORARY_FAILURE,
            "[JOBID %s] Job failed temporarily: GET 503" % job_id)
        queue_processor.process_with_pool(1, _read_default_db_ini())
        last_started_at = self._get_last_started_at(job_id)
        eq_(0, self._get_remaining_retries(job_id))
        queue_processor.process_with_pool(1, _read_default_db_ini())
        eq_(0, self._get_remaining_retries(job_id))
        second_last_started_at = self._get_last_started_at(job_id)
        # the job should not have been re-worked
        eq_(last_started_at, second_last_started_at)

    # help protect against deadlock
    @timed(10)
    def test_delay_secs(self):
        # this is crappy time based stuff, and yet it's better than
        # not testing IMHO.
        job_id = self._queue_job('get', '/test', retry_delay_secs=3)
        self._start_server(_make_handler_class('TestDelaysHandler', 503))
        queue_processor.process_with_pool(1, _read_default_db_ini())
        self._assert_done(
            job_id,
            queue_processor.TEMPORARY_FAILURE,
            "[JOBID %s] Job failed temporarily: GET 503" % job_id)
        last_started_at = self._get_last_started_at(job_id)
        queue_processor.process_with_pool(1, _read_default_db_ini())
        second_last_started_at = self._get_last_started_at(job_id)
        # the job should not have been re-worked, as we should be
        # safely within the 3 second delay
        eq_(last_started_at, second_last_started_at)
        time.sleep(5)
        queue_processor.process_with_pool(1, _read_default_db_ini())
        # now the job should have been reworked
        third_last_started_at = self._get_last_started_at(job_id)
        ok_(last_started_at != third_last_started_at)

    # help protect against deadlock
    @timed(10)
    def test_time_out(self):
        # this is crappy time based stuff, and yet it's better than
        # not testing IMHO.
        def _sleep_little_baby(other_self):
            time.sleep(5)
        job_id = self._queue_job('get', '/test', timeout_secs=1)
        self._start_server(_make_handler_class('TestTimeoutHandler', 503,
                                               do_GET=_sleep_little_baby))
        queue_processor.process_with_pool(1, _read_default_db_ini())
        self._assert_done(
            job_id,
            queue_processor.TEMPORARY_FAILURE,
            "[JOBID %s] Job failed due to timeout" % job_id)

    # help protect against deadlock
    @timed(10)
    def test_respects_new_retry_delay_secs(self):
        def _new_retry_delay_seconds(other_self):
            other_self.send_response(503)
            other_self.send_header('x-bitlancer-retry-delay-secs','786')
            other_self.end_headers()
            other_self.wfile.write("GET 503")

        job_id = self._queue_job('get', '/test', retry_delay_secs=10)
        eq_(10, self._get_retry_delay_secs(job_id))
        self._start_server(_make_handler_class('TestRetryDelaySeconds', 503,
                                               do_GET=_new_retry_delay_seconds))
        queue_processor.process_with_pool(1, _read_default_db_ini())
        eq_(786, self._get_retry_delay_secs(job_id))

    # help protect against deadlock
    @timed(10)
    def test_multiple_jobs(self):
        job_id_one = self._queue_job('post', '/test', body="this is a test body")
        job_id_two = self._queue_job('get', '/test')
        self._start_server(_make_handler_class('TestMultipleJobs', 200))
        # make sure that even running with 1 process, we do both jobs
        queue_processor.process_with_pool(1, _read_default_db_ini())
        self._assert_done(
            job_id_one,
            queue_processor.SUCCESS,
            "[JOBID %s] Job succeeded: POST 200" % job_id_one)
        self._assert_done(
            job_id_two,
            queue_processor.SUCCESS,
            "[JOBID %s] Job succeeded: GET 200" % job_id_two)

    ################
    # HELPER FUNCS #
    ################

    def _get_last_started_at(self, job_id):
        curs = self.conn.cursor()
        curs.execute("SELECT last_started_at FROM queued_job WHERE id = %s",
                     (job_id,))
        return curs.fetchone()[0]

    def _get_remaining_retries(self, job_id):
        curs = self.conn.cursor()
        curs.execute("SELECT remaining_retries FROM queued_job WHERE id = %s",
                     (job_id,))
        return curs.fetchone()[0]

    def _get_retry_delay_secs(self, job_id):
        curs = self.conn.cursor()
        curs.execute("SELECT retry_delay_secs FROM queued_job WHERE id = %s",
                     (job_id,))
        return curs.fetchone()[0]


    def _assert_done(self, job_id, status, text):
        curs = self.conn.cursor()
        curs.execute("SELECT result_code FROM queued_job WHERE id = %s", (job_id,))
        result_code = curs.fetchone()[0]
        eq_(status, result_code)
        curs.execute("""SELECT msg
                          FROM queued_job_log
                          WHERE job_id = %s
                          ORDER BY id DESC
                          LIMIT 1""",
                     (job_id,))
        msg = curs.fetchone()[0]
        eq_(text, msg)

    def _queue_job(self, method, uri, body=None, timeout_secs=10, remaining_retries=10,
                   retry_delay_secs=0):
        curs = self.conn.cursor()
        global port
        curs.execute("""
                     INSERT INTO queued_job
                       (http_method, url, body, timeout_secs, remaining_retries,
                        retry_delay_secs)
                     VALUES
                       (%s, %s, %s, %s, %s,
                        %s)
                     """,
                     (method, "http://127.0.0.1:%d%s" % (port, uri),
                      body, timeout_secs, remaining_retries,
                      retry_delay_secs))
        curs.execute("SELECT LAST_INSERT_ID()")
        return curs.fetchone()[0]

