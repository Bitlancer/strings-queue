CREATE TABLE IF NOT EXISTS `queued_job` (
       `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'The id of the queued job',
       `http_method` VARCHAR(10) NOT NULL COMMENT 'The HTTP VERB that will be used to call this job',
       `url` TEXT NOT NULL COMMENT 'The callback url',
       `body` TEXT NULL COMMENT 'Content sent to the job',
       `timeout_secs` INT NOT NULL DEFAULT 60 COMMENT 'Job execution timeout',
       `last_started_at` TIMESTAMP NULL COMMENT 'Last time this job was run',
       `last_finished_at` TIMESTAMP NULL COMMENT 'Last time this job completed',
       `result_code` INT NULL COMMENT 'HTTP status code',
       `remaining_retries` INT NOT NULL DEFAULT 10 COMMENT 'Number of remaining retries before the job is marked as failed',
       `retry_delay_secs` INT NOT NULL DEFAULT 60 COMMENT 'Do not retry this job for this number of seconds',
       PRIMARY KEY (`id`));

CREATE TABLE IF NOT EXISTS `queued_job_log` (
       `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'The id of the log entry',
       `job_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'The id of job referenced',
       `msg` TEXT NOT NULL COMMENT 'The result returned from the executed job',
       `created_at` TIMESTAMP NOT NULL,
       PRIMARY KEY (`id`));
