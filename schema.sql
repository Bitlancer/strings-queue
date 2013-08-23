CREATE TABLE `queued_job` (
       `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'The id of the queued job',
	`organization_id` bigint(20) unsigned DEFAULT NULL COMMENT 'The id of the organization that owns this record',
	`http_method` varchar(10) NOT NULL COMMENT 'The HTTP VERB that will be used to call this job',
	`url` text NOT NULL COMMENT 'The callback url',
	`body` text COMMENT 'Content sent to the callback url in the POST body',
	`timeout_secs` int(11) NOT NULL DEFAULT '60' COMMENT 'Job execution timeout',
	`last_started_at` timestamp NULL DEFAULT NULL COMMENT 'Last time this job was run',
	`last_finished_at` timestamp NULL DEFAULT NULL COMMENT 'Last time this job completed',
        `last_response` text COMMENT 'The body from the last response of the last execution',
	`result_code` int(11) DEFAULT NULL COMMENT 'HTTP status code',
	`remaining_retries` int(11) NOT NULL DEFAULT '10' COMMENT 'Number of remaining retries before the job is marked as failed',
	`retry_delay_secs` int(11) NOT NULL DEFAULT '60' COMMENT 'Do not retry this job for this number of seconds',
	PRIMARY KEY (`id`))

CREATE TABLE IF NOT EXISTS `queued_job_log` (
       `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'The id of the log entry',
       `job_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'The id of job referenced',
       `msg` TEXT NOT NULL COMMENT 'The result returned from the executed job',
       `created_at` TIMESTAMP NOT NULL,
       PRIMARY KEY (`id`));
