-- UP
-- create new response table
CREATE TABLE `etl_webservice_response` (
    `oid` BINARY(16) NOT NULL,
    `created_on` DATETIME(6) NOT NULL,
    `modified_on` DATETIME(6) NOT NULL,
    `created_by_user_oid` BINARY(16) NOT NULL,
    `modified_by_user_oid` BINARY(16) NOT NULL,
    `webservice_request_oid` BINARY(16) NULL,
    `http_response_code` SMALLINT NOT NULL,
    `http_content_type` VARCHAR(200) NULL,
    `status` TINYINT NOT NULL DEFAULT 10,
    `result_text` TEXT NULL,
    `error_logid` VARCHAR(20) NULL,
    `error_message` TEXT NULL,
    `response_body` TEXT NULL,
    `response_header` TEXT NULL,
    PRIMARY KEY (`oid`)
);
ALTER TABLE `etl_webservice_response`
ADD CONSTRAINT `fk_webservice_response_request` FOREIGN KEY (`webservice_request_oid`)
    REFERENCES `etl_webservice_request`(`oid`);
-- move data from request to response table
INSERT INTO `etl_webservice_response` (
    oid, created_on, modified_on, created_by_user_oid, modified_by_user_oid, webservice_request_oid, http_response_code, http_content_type,
    status, result_text, error_logid, error_message, response_body, response_header
)
SELECT
    r.oid, r.created_on, r.modified_on, r.created_by_user_oid, r.modified_by_user_oid, r.oid AS webservice_request_oid,
    r.http_response_code,
    IF(JSON_VALID(r.response_header), JSON_UNQUOTE(JSON_EXTRACT(r.response_header, '$."Content-Type"[0]')), NULL) AS http_content_type,
    r.status, r.result_text, r.error_logid, r.error_message, r.response_body, r.response_header
FROM `etl_webservice_request` r
WHERE r.http_response_code IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM `etl_webservice_response` e WHERE e.oid = r.oid);
-- add new column to table
ALTER TABLE `etl_webservice_request` ADD COLUMN `http_querystring` TEXT NULL;
-- drop request columns
ALTER TABLE `etl_webservice_request` DROP COLUMN `http_response_code`;
ALTER TABLE `etl_webservice_request` DROP COLUMN `result_text`;
ALTER TABLE `etl_webservice_request` DROP COLUMN `response_body`;
ALTER TABLE `etl_webservice_request` DROP COLUMN `response_header`;
-- DOWN
-- recreate columns
ALTER TABLE `etl_webservice_request`
    ADD COLUMN `http_response_code` SMALLINT NULL DEFAULT 10,
ADD COLUMN `result_text` TEXT NULL,
ADD COLUMN `response_body` TEXT NULL,
ADD COLUMN `response_header` TEXT NULL;
-- drop new column from table
ALTER TABLE `etl_webservice_request` DROP COLUMN `http_querystring`;
-- move data from response to request table
UPDATE `etl_webservice_request` trg
    JOIN `etl_webservice_response` src
ON src.webservice_request_oid = trg.oid
    SET trg.http_response_code = src.http_response_code,
        trg.result_text = src.result_text,
        trg.response_body = src.response_body,
        trg.response_header = src.response_header,
        trg.http_content_type = src.http_content_type;
-- drop new table for responses
DROP TABLE `etl_webservice_response`;
