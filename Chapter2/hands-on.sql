------------------------------------
-- CHAPTER 2 HANDS-ON SQL OPERATIONS
------------------------------------

-- create and use a new schema for this chapter
CREATE SCHEMA optimize_ice.ch2;
USE optimize_ice.ch2;

-- create an empty Iceberg table
CREATE TABLE my_iceberg_tbl (
   id integer,
   name varchar(55),
   description varchar(255)
) WITH (TYPE = 'iceberg', FORMAT = 'parquet');

-- verify there is a single snapshot w/o any parent
SELECT
  snapshot_id,
  parent_id,
  substring(manifest_list, 
            position('/metadata/' IN manifest_list) + 10)
   AS manifest_list
FROM
  "my_iceberg_tbl$snapshots";

-- insert 3 records (will be in a single file)
INSERT INTO my_iceberg_tbl
 (id, name, description)
VALUES
 (101, 'Leto', 'Ruler of House Atreides'),
 (102, 'Jessica', 'Consort of the Duke'),
 (103, 'Paul', 'Son of Leto (aka Dale Cooper)');

-- verify 3 records present
SELECT * FROM my_iceberg_tbl;

-- verify a 2nd snapshot 
SELECT
  snapshot_id,
  parent_id,
  substring(manifest_list, 
            position('/metadata/' IN manifest_list) + 10)
   AS manifest_list
FROM
  "my_iceberg_tbl$snapshots";

-- view the single manifest w/1 file and 3 records
SELECT
  substring(path, 
            position('/metadata/' IN path) + 10)
   AS path,
  added_snapshot_id,
  added_data_files_count,
  added_rows_count
FROM
  "my_iceberg_tbl$manifests";

-- view the ref'd file and its statistics
SELECT
 substring(file_path, 
           position('/data/' IN file_path) + 6)
   AS file_path,
 record_count,
 value_counts,
 null_value_counts,
 lower_bounds,
 upper_bounds
FROM
 "my_iceberg_tbl$files";

-- add 5 more records
INSERT INTO my_iceberg_tbl
   (id, name, description)
VALUES
   (104, 'Thufir', 'Mentat'),
   (201, 'Vladimir', 'Ruler of House Harkonnen'),
   (202, 'Rabban', 'Ruthless nephew of Vladimir'),
   (203, 'Feyd-Rautha', 'Savvy nephew of Vladimir'),
   (301, 'Reverend Mother Gaius Helen Mohiam', null);

-- verify 8 records present now
SELECT * FROM my_iceberg_tbl;

-- see the 3rd snapshot now
SELECT
  snapshot_id,
  parent_id,
  substring(manifest_list, 
            position('/metadata/' IN manifest_list) + 10)
   AS manifest_list
FROM
  "my_iceberg_tbl$snapshots";

-- view the second manifest w/1 file and 5 records
SELECT
  substring(path, 
            position('/metadata/' IN path) + 10)
   AS path,
  added_snapshot_id,
  added_data_files_count,
  added_rows_count
FROM
  "my_iceberg_tbl$manifests";

-- view the 2nd ref'd file and its statistics
SELECT
 substring(file_path, 
           position('/data/' IN file_path) + 6)
   AS file_path,
 record_count,
 value_counts,
 null_value_counts,
 lower_bounds,
 upper_bounds
FROM
 "my_iceberg_tbl$files";

-- run this insert statement 10 times
INSERT INTO my_iceberg_tbl
  SELECT * FROM my_iceberg_tbl;

-- make sure you have at least 12 files present
SELECT
  file_path,
  record_count,
  file_size_in_bytes
FROM
  "my_iceberg_tbl$files";

-- kick off the compaction task
ALTER TABLE my_iceberg_tbl EXECUTE optimize;

-- verify there are fewer, larger files (likely will be 4)
SELECT
  file_path,
  record_count,
  file_size_in_bytes
FROM
  "my_iceberg_tbl$files";
