strings-queue
=============

Bitlancer Strings Queue

## Installation

Requires:

- python 2.6 (or higher, theoretically), including devel packages
- mysql (including devel packages)
- python pip

Once these are installed, a

    pip install -r requirements.txt

as an appropriately credentialed user ought to take care of the rest.

## Db setup

The schema.sql file contains all the table creations you'll need.

You also need to create an ini file with db information (take a look
db.example.ini for format).

## Running

Currently, this has to be run in the main strings-queue directory
(I'll work on changing that).

You can run with ./run.sh <db_ini_file>.

Run with ./run.sh -h for help / options.

## Logging etc.

Everything (including diagnostic information, start / stop run, etc.)
is logged to standard error, and events pertaining to jobs are also
logged to the queued_job_log table, keyed off the job id.

## Adding jobs to the queue.

Take a look at schema.sql.  Adding something to the queue just means
adding it to the queued_job table, e.g.

INSERT INTO queued_job
  (http_method, url, body, timeout_secs)
VALUES
  ('post', 'http://something.example.com/somewhere', 'some post body', 120);

## Writing a handler

There are really only three rules to keep in mind for writing a
handler, and two tips.

### Rules

- Return either 200 or an error code.  503 will be treated as a
  temporary failure and retried up to 5 times (as will a timeout).
  200 is considered successful completion of the job.

- Return a pithy, one or two line string if you'd like in the http
  body, as plain text.  This will get logged in the queue processor
  and the queued_job_log table.

- Since it's always possible that the connection is broken or
  something fails in the middle of processing, which the queue will
  attempt to retry, you should write your jobs so it doesn't blow up
  the world if they get called twice.

### Tips

- The default job timeout is 60 seconds.  Make sure to set it higher
  for jobs you expect to take longer.

- Make sure that the web stack in front of your requests doesn't have
  any short timeouts set (e.g. load balancer, apache or similar, php
  module or similar).
