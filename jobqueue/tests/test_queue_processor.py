"""
Test the queue processor.
"""

from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
from ConfigParser import SafeConfigParser
import os
import threading
import unittest

from nose.tools import ok_, eq_

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

    def test_simple_endpoint(self):
        self._do_test_job_simple(
            200,
            queue_processor.SUCCESS,
            "[JOBID %(job_id)s] Job succeeded: GET %(resp_code)s")

    def test_temporary_failure(self):
        self._do_test_job_simple(
            503,
            queue_processor.TEMPORARY_FAILURE,
            "[JOBID %(job_id)s] Job failed temporarily: GET %(resp_code)s")

    def test_permanent_failure(self):
        self._do_test_job_simple(
            500,
            queue_processor.PERMANENT_FAILURE,
            "[JOBID %(job_id)s] Job failed permanently: GET %(resp_code)s")

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



    # TODO: test timeout

    # TODO: test multiple jobs

    # TODO: test body received on post

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

    def _queue_job(self, method, uri, body=None, timeout_secs=10):
        curs = self.conn.cursor()
        global port
        curs.execute("""
                     INSERT INTO queued_job
                       (http_method, url, body, timeout_secs)
                     VALUES
                       (%s, %s, %s, %s)
                     """,
                     (method, "http://127.0.0.1:%d%s" % (port, uri),
                      body, timeout_secs))
        curs.execute("SELECT LAST_INSERT_ID()")
        return curs.fetchone()[0]

