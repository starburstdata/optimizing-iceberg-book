------------------------------------
-- CHAPTER 3 HANDS-ON SQL OPERATIONS
------------------------------------

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

-- simple partitioned table
CREATE TABLE customer
WITH (
  type = 'iceberg', format_version = 3,
  partitioning = ARRAY['mktsegment']
)
AS SELECT custkey, name, mktsegment, phone 
     FROM tpch.tiny.customer;

SELECT partition, record_count, file_count
  FROM "customer$partitions";

-- partition table using day(ts) transform function
CREATE TABLE orders (
  order_id bigint, cust_id bigint, order_pri varchar,
  order_ts timestamp, 
  order_amt double 
)
WITH (
  type ='iceberg', format_version = 3,
  partitioning = ARRAY['day(order_ts)']
);

-- insert into a single partition
INSERT INTO orders
  SELECT orderkey, custkey, orderpriority,
         current_timestamp(6) - interval '1' day, 
         totalprice
    FROM tpch.sf1.orders;

-- verify there are 1.5M rows
SELECT format_number(COUNT(*)) FROM orders;

-- 4 more insert commands filling up 4 more partitions
INSERT INTO orders
  SELECT orderkey, custkey, orderpriority,
         current_timestamp(6) - interval '2' day, 
         totalprice
    FROM tpch.sf1.orders;

INSERT INTO orders
  SELECT orderkey, custkey, orderpriority,
         current_timestamp(6) - interval '3' day, 
         totalprice
    FROM tpch.sf1.orders;

INSERT INTO orders
  SELECT orderkey, custkey, orderpriority,
         current_timestamp(6) - interval '4' day, 
         totalprice
    FROM tpch.sf1.orders;

INSERT INTO orders
  SELECT orderkey, custkey, orderpriority,
         current_timestamp(6) - interval '5' day, 
         totalprice
    FROM tpch.sf1.orders;

-- verify 5 partitions are present with 1.5M rows for each
SELECT partition, 
       format_number(record_count) AS row_count
  FROM "orders$partitions";

-- change partitioning
ALTER TABLE orders
SET PROPERTIES partitioning = ARRAY['order_pri'];

-- verify partitiong change is present
SHOW CREATE TABLE orders;

-- write into the new partitions
INSERT INTO orders
  SELECT orderkey, custkey, orderpriority,
         current_timestamp(6) - interval '10' day, 
         totalprice
    FROM tpch.sf1.orders;

-- verify 5 partitions are present with 1.5M rows for each
SELECT partition, 
       format_number(record_count) AS row_count
  FROM "orders$partitions";

-- verify 9M rows present now
SELECT format_number(count()) FROM orders;

-- should have 96 rows for customer 82661 
SELECT * FROM orders WHERE cust_id = 82661;

-- verify you can see all 96 again via time-travel
SELECT * FROM orders FOR TIMESTAMP AS OF
    current_timestamp(6) - interval '5' minute
 WHERE cust_id = 82661;

-- DELETE all 96 of them
DELETE FROM orders WHERE cust_id = 82661;

-- list snapshots
SELECT committed_at, snapshot_id, parent_id
  FROM "orders$snapshots" 
 ORDER BY committed_at;

-- verify you can see all 96 again via time-travel
--  **** REPLACE 44444444444 WITH THE snapshot_id
--  ****  from the 2nd to last row in the prior Q
SELECT * FROM orders FOR VERSION AS OF 44444444444
 WHERE cust_id = 82661;

-- rollback to the cust_id used in the last Q
ALTER TABLE orders EXECUTE 
  rollback_to_snapshot(44444444444);

-- get the current snapshot ID (should be what we rolled back to)
SELECT name, type, snapshot_id FROM "orders$refs"
 WHERE type='BRANCH' AND name='main';


-- BRANCHING ONLY WORKING IN STARBURST ENTERPRISE 
--  AS THE BOOK TEXT CALLS OUT
SET SESSION skip_results_cache = true;

-- create and populate the table
CREATE TABLE branching (data INT, part DATE) 
WITH (partitioning = ARRAY['part']);

INSERT INTO branching VALUES 
   (10,  DATE '2026-01-01'), 
   (20,  DATE '2026-01-02'),
   (-30, DATE '2026-01-03'),
   (40,  DATE '2026-01-04'),
   (50,  DATE '2026-01-05');

-- create branch
CREATE BRANCH dev IN TABLE branching;
SHOW BRANCHES FROM TABLE branching;

-- delete partition and backfill it
DELETE FROM branching @ dev WHERE part = DATE '2026-01-03';
INSERT INTO branching @ dev VALUES (30, DATE '2026-01-03');
SELECT * FROM branching FOR VERSION AS OF 'dev';

-- 2 ways to query trunk
SELECT * FROM branching;
SELECT * FROM branching FOR VERSION AS OF 'main';

-- merge the branch's data back to the trunk
ALTER BRANCH main IN TABLE branching FAST FORWARD TO dev;

-- verify changes are in trunk
SELECT * FROM branching;