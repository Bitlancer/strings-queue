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


class TestHttpRequestHandler200(BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type','text/text')
        self.end_headers()
        self.wfile.write("GET 200")

    def do_POST(self):
        self.send_response(200)
        self.send_header('Content-type','text/text')
        self.end_headers()
        self.wfile.write("POST 200")

    def log_message(self, *args, **kwargs):
        """
        Keep messages out of nosetests output.
        """
        pass


class TestHttpRequestHandler503(BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(503)
        self.send_header('Content-type','text/text')
        self.end_headers()
        self.wfile.write("GET 503")

    def do_POST(self):
        self.send_response(503)
        self.send_header('Content-type','text/text')
        self.end_headers()
        self.wfile.write("POST 503")

    def log_message(self, *args, **kwargs):
        """
        Keep messages out of nosetests output.
        """
        pass


class TestHttpRequestHandler500(BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(500)
        self.send_header('Content-type','text/text')
        self.end_headers()
        self.wfile.write("GET 500")

    def do_POST(self):
        self.send_response(500)
        self.send_header('Content-type','text/text')
        self.end_headers()
        self.wfile.write("POST 500")

    def log_message(self, *args, **kwargs):
        """
        Keep messages out of nosetests output.
        """
        pass


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

    def test_simple_endpoint(self):
        job_id = self._queue_job('get', '/test')
        self._start_server(TestHttpRequestHandler200)
        queue_processor.process_with_pool(1, _read_default_db_ini())
        self._assert_done(job_id, queue_processor.SUCCESS,
                          "[JOBID %s] Job succeeded: GET 200" % job_id)

    def test_temporary_failure(self):
        job_id = self._queue_job('get', '/test')
        self._start_server(TestHttpRequestHandler503)
        queue_processor.process_with_pool(1, _read_default_db_ini())
        self._assert_done(job_id, queue_processor.TEMPORARY_FAILURE,
                          "[JOBID %s] Job failed temporarily: GET 503" % job_id)

    def test_permanent_failure(self):
        job_id = self._queue_job('get', '/test')
        self._start_server(TestHttpRequestHandler500)
        queue_processor.process_with_pool(1, _read_default_db_ini())
        self._assert_done(job_id, queue_processor.PERMANENT_FAILURE,
                          "[JOBID %s] Job failed permanently: GET 500" % job_id)

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

