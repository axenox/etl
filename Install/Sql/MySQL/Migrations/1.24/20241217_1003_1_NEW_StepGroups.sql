-- UP

ALTER TABLE `etl_step`
ADD `parent_step_oid` binary(16) NULL;

ALTER TABLE `etl_step`
CHANGE `to_object_oid` `to_object_oid` binary(16) NULL AFTER `from_object_oid`;

-- DOWN

