------------------------------------
-- CHAPTER 3 HANDS-ON SQL OPERATIONS
------------------------------------

-- create and use a new schema for this chapter
CREATE SCHEMA optimize_ice.ch3;
USE optimize_ice.ch3;

-- create and use a new schema for this chapter
CREATE SCHEMA optimize_ice.ch3;
USE optimize_ice.ch3;

-- CTAS to perform DML on
CREATE TABLE dml_region 
WITH (TYPE='iceberg', format_version=2)
AS SELECT * FROM tpch.sf1.region;

-- see the initial 5 records
SELECT regionkey AS key, name, comment FROM dml_region;

-- verify a single file w/5 records present
SELECT file_path, record_count FROM "dml_region$files";

-- insert 4 records (will be in a single file)
INSERT INTO dml_region
 (regionkey, name, comment)
VALUES
 (5, 'Middle-Earth', 'J.R.R. Tolkien'),
 (6, 'Narnia', 'C.S. Lewis'),
 (7, 'Erilea', 'Sarah J. Maas'),
 (8, 'Westeros', 'George R.R. Martin');
-- what's up with all the initials?

-- see the additional 4 records
SELECT regionkey AS key, name, comment 
  FROM dml_region WHERE regionkey > 4;

-- verify a second file shows up with 4 new records
SELECT file_path, record_count FROM "dml_region$files";

-- delete the one that doesn't match the others
--  (not enough initials)
DELETE FROM dml_region WHERE regionkey = 7;

-- verify #7 was deleted
SELECT regionkey AS key, name, comment 
  FROM dml_region WHERE regionkey > 4;

-- verify a new, smaller file, is present that indicates 1 record
--  which is the POSITIONAL DELETE FILE
SELECT file_path, record_count FROM "dml_region$files";

-- update the 3 records that start with A
UPDATE dml_region
   SET comment = 'starts with A'
 WHERE name LIKE 'A%';

-- verify first 3 records were changed
SELECT regionkey AS key, name, comment 
  FROM dml_region WHERE name LIKE 'A%';

-- verify another delete file and 1 new data files are created
SELECT
 substring(file_path, 
           position('/data/' IN file_path) + 6)
   AS file_path,
 record_count
FROM
 "dml_region$files";
