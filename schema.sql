CREATE TABLE IF NOT EXISTS `queued_job` (
       `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'The id of the queued job',
       `http_method` VARCHAR(10) NOT NULL,
       `url` TEXT NOT NULL,
       `body` TEXT NULL,
       `timeout_secs` INT NOT NULL DEFAULT 60,
       `last_started_at` TIMESTAMP NULL,
       `last_finished_at` TIMESTAMP NULL,
       `result_code` INT NULL,
       `remaining_retries` INT NOT NULL DEFAULT 10,
       `retry_delay_secs` INT NOT NULL DEFAULT 60,
       PRIMARY KEY (`id`));

CREATE TABLE IF NOT EXISTS `queued_job_log` (
       `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'The id of the log entry',
       `job_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'The id of job referenced',
       `msg` TEXT NOT NULL,
       `created_at` TIMESTAMP NOT NULL,
       PRIMARY KEY (`id`));