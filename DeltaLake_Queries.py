# Databricks notebook source
# MAGIC %md
# MAGIC ## Delta Table Operations
# MAGIC
# MAGIC 1. Create a table.
# MAGIC 1. Upsert to a table.
# MAGIC 1. Read from a table.
# MAGIC 1. Display table history.
# MAGIC 1. Query an earlier version of a table.
# MAGIC 1. Optimize a table.
# MAGIC 1. Add a Z-order index.
# MAGIC 1. Vacuum unreferenced files.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Table Demo

# COMMAND ----------

table_file_path="/databricks-datasets/learning-spark-v2/people/people-10m.delta"
ext_dir_path="/files/ext_tables/"

# COMMAND ----------

dbutils.fs.ls(ext_dir_path)

# COMMAND ----------

dbutils.fs.rm(ext_dir_path,recurse=True)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### Python Managed Table

# COMMAND ----------

df=spark.read.format('delta').load(table_file_path)
df.write.mode("overwrite").saveAsTable("managed_people_ds")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended managed_people_ds;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Python External Table
# MAGIC
# MAGIC Directory mentioned as part of "path" will be table name.

# COMMAND ----------

df.write.mode("overwrite").option("path",ext_dir_path+'ext_people').save();

# COMMAND ----------

# MAGIC %fs ls ext_dir_path+"ext_people"

# COMMAND ----------

ext_dir_path

# COMMAND ----------

# MAGIC %sql
# MAGIC desc table extended ext_people

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL External Table

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table if not exists ext_people 
# MAGIC location '/files/ext_tables/ext_people/'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended ext_people

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Table Syntax

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL Syntax

# COMMAND ----------

# MAGIC     %md
# MAGIC     { { [CREATE OR] REPLACE TABLE | CREATE [EXTERNAL] TABLE [ IF NOT EXISTS ] } table_name
# MAGIC       [ table_specification ]
# MAGIC       [ USING data_source ]
# MAGIC       [ table_clauses ]
# MAGIC       [ AS query ] }
# MAGIC
# MAGIC     table_specification
# MAGIC       ( { column_identifier column_type [ column_properties ] ] } [, ...]
# MAGIC         [ , table_constraint ] [...] )
# MAGIC
# MAGIC     column_properties
# MAGIC       { NOT NULL |
# MAGIC         GENERATED ALWAYS AS ( expr ) |
# MAGIC         GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY [ ( [ START WITH start ] [ INCREMENT BY step ] ) ] |
# MAGIC         DEFAULT default_expression |
# MAGIC         COMMENT column_comment |
# MAGIC         column_constraint |
# MAGIC         MASK clause } [ ... ]
# MAGIC
# MAGIC     table_clauses
# MAGIC       { OPTIONS clause |
# MAGIC         PARTITIONED BY clause |
# MAGIC         clustered_by_clause |
# MAGIC         LOCATION path [ WITH ( CREDENTIAL credential_name ) ] |
# MAGIC         COMMENT table_comment |
# MAGIC         TBLPROPERTIES clause |
# MAGIC         WITH { ROW FILTER clause } } [...]
# MAGIC
# MAGIC     clustered_by_clause
# MAGIC       { CLUSTERED BY ( cluster_column [, ...] )
# MAGIC         [ SORTED BY ( { sort_column [ ASC | DESC ] } [, ...] ) ]
# MAGIC         INTO num_buckets BUCKETS }

# COMMAND ----------

# MAGIC %md
# MAGIC **REPLACE**
# MAGIC
# MAGIC This clause is only supported for Delta Lake tables.REPLACE preserves the table history. Databricks strongly recommends using REPLACE instead of dropping and re-creating Delta Lake tables. IF NOT EXISTS cannot coexist with REPLACE, which means CREATE OR REPLACE TABLE IF NOT EXISTS is not allowed. Replace cant be used with external tables.
# MAGIC
# MAGIC If you do not define columns the table schema you must specify either AS query or LOCATION.

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL Managed Table Example

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table people_ds_sql
# MAGIC as
# MAGIC select * from delta.`/databricks-datasets/learning-spark-v2/people/people-10m.delta`

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL External Table Example

# COMMAND ----------

# MAGIC %sql
# MAGIC create  external table people_ext_sql
# MAGIC Location '/databricks-datasets/learning-spark-v2/people/people-10m.delta'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Table Like 
# MAGIC
# MAGIC CREATE TABLE prod.people10m LIKE dev.people10m

# COMMAND ----------

# MAGIC %md
# MAGIC #### DeltaTableBuilder API (Python)
# MAGIC
# MAGIC
# MAGIC
# MAGIC     DeltaTable.createIfNotExists(spark).tableName("TableName").addColumn("Col_Name","Col_DataType",
# MAGIC     comment="comment for column")
# MAGIC     .addColumn( "Next Col Name","Column Datatype").execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists people_empty_py;

# COMMAND ----------

from delta.tables import *
DeltaTable.createIfNotExists(spark).tableName("people_empty_py").addColumn("Id","int",comment="new col").addColumn( "today","Timestamp").execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table DML Operations

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing to Table

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SQL Write to table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into people_empty_py
# MAGIC select 1,current_timestamp() from people_empty_py;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into people_empty_py
# MAGIC values (1,current_date()),(2,now());

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Python Writing to Table

# COMMAND ----------

n=spark.sql("select current_timestamp() as DT").collect()[0]["DT"]
from pyspark.sql.types import *
df=[[int(200),n]]
sch=StructType().add("Id","integer").add("today","timestamp")
df=sc.parallelize(df).toDF(sch)
df.write.mode("append").saveAsTable("people_empty_py")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read a table
# MAGIC Lets us say we have table named "people_empty_py" with data stored in delta format on "/user/hive/warehouse/people_empty_py". Table have 2 columns (ID,Today).You access data in Delta tables by 
# MAGIC 1. table name 
# MAGIC
# MAGIC     SQL (select * from people_empty_py) 
# MAGIC
# MAGIC     Python (spark.read.table("people_empty_py"))
# MAGIC
# MAGIC 1. table path 
# MAGIC
# MAGIC     SQL (select * from delta.`/user/hive/warehouse/people_empty_py`)
# MAGIC
# MAGIC     Python (spark.read.format("delta").load("/user/hive/warehouse/people_empty_py"))

# COMMAND ----------

##display(spark.read.table("people_empty_py"))---name based
display(spark.read.format('delta').load("/user/hive/warehouse/people_empty_py"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Update a table
# MAGIC

# COMMAND ----------

##### SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC update delta.`/user/hive/warehouse/people_empty_py`
# MAGIC set id=1000
# MAGIC where id=100;
# MAGIC
# MAGIC select * from people_empty_py;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Python

# COMMAND ----------

from pyspark.sql.functions import  *
df=spark.read.table('people_empty_py').withColumn("Id",when(col("Id")==1000,2000).otherwise(col("Id")) )
df.write.mode("overwrite").saveAsTable("people_empty_py")

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from people_empty_py

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Python DeltaTable API

# COMMAND ----------

from delta.tables import *
df=DeltaTable.forName(spark,'people_empty_py');
df.update(condition="Id=2000",set={"Id":"1000"})

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC #### Delete from a table
# MAGIC
# MAGIC delete removes the data from the latest version of the Delta table but does not remove it from the physical storage until the old versions are explicitly vacuumed.

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from delta.`/user/hive/warehouse/people_empty_py`
# MAGIC where id>=1000;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Python DeltaBuilder API

# COMMAND ----------

df=DeltaTable.forName(spark,"people_empty_py");
df.delete("id>=1000");

# COMMAND ----------

# MAGIC %md
# MAGIC #### Upserting Table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from people_ds_sql where id>=9999998

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW people_updates (
# MAGIC   id, firstName, middleName, lastName, gender, birthDate, ssn, salary
# MAGIC ) AS VALUES
# MAGIC   (9999998, 'Ali', 'Shahbaz', 'Shahid', 'M', '1992-09-17T04:00:00.000', '953-38-9452', 55250),
# MAGIC   (9999999, 'Ahmed', 'Ramzy', 'Ibrahim', 'M', '1984-05-22T04:00:00.000', '906-51-2137', 48500),
# MAGIC   (10000000, 'Joshua', 'Chas', 'Broggio', 'M', '1968-07-22T04:00:00.000', '988-61-6247', 90000),
# MAGIC   (20000001, 'John', '', 'Doe', 'M', '1978-01-14T04:00:00.000', '345-67-8901', 55500),
# MAGIC   (20000002, 'Mary', '', 'Smith', 'F', '1982-10-29T01:00:00.000', '456-78-9012', 98250),
# MAGIC   (20000003, 'Jane', '', 'Doe', 'F', '1981-06-25T04:00:00.000', '567-89-0123', 89900);
# MAGIC
# MAGIC MERGE into people_ds_sql tgt using people_updates src on tgt.id=src.id
# MAGIC when matched then update set *
# MAGIC when not matched  then insert *
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Insert Overwrite
# MAGIC To atomically replace all the data in a table, use overwrite mode as in the following examples:
# MAGIC
# MAGIC **SQL**
# MAGIC
# MAGIC INSERT OVERWRITE TABLE target_Table SELECT * FROM srcTable
# MAGIC
# MAGIC **Python**
# MAGIC
# MAGIC srcTable_df.write.mode("overwrite").saveAsTable("target_Table")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE people_empty_py(Id,today)
# MAGIC select 1,now();
# MAGIC select * from people_empty_py;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Table History Demo

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history  people_empty_py;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Restore Table Demo

# COMMAND ----------

# MAGIC %sql
# MAGIC restore table people_empty_py to version as of  9;
# MAGIC select * from people_empty_py;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta Lake table history
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC         DESCRIBE HISTORY '/user/hive/warehouse/people_empty_py/'          -- get the full history of the table
# MAGIC
# MAGIC         DESCRIBE HISTORY '/user/hive/warehouse/people_empty_py/' LIMIT 1  -- get the last operation only
# MAGIC
# MAGIC         DESCRIBE HISTORY delta.`/user/hive/warehouse/people_empty_py/`
# MAGIC
# MAGIC         DESCRIBE HISTORY people_empty_py

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history  people_empty_py;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query an earlier version of the table (time travel)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from people_empty_py version as of 9;
# MAGIC --select * from people_empty_py timestamp as of '2023-10-20T08:08:50.335+0000'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Python

# COMMAND ----------

display(spark.read.option("versionAsOf",9).table("people_empty_py"));
##display(spark.read.option("TimestampAsOf",'2023-10-20T08:08:50.335+0000').table("people_empty_py"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restore a Delta table to an earlier state (RESTORE)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC restore table tblName version as of versionNumber;
# MAGIC restore table tblName timestamp as of '2023-10-21';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimize a table
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Syntax
# MAGIC
# MAGIC ###### SQL
# MAGIC
# MAGIC     optimize tableName;
# MAGIC     optimize tableFormat.`tablePath`;
# MAGIC
# MAGIC ###### Python
# MAGIC
# MAGIC     DeltaTable.forName(spark,'tableName').optimize().executeCompaction()
# MAGIC
# MAGIC If you have a large amount of data and only want to optimize a subset of it, you can specify an optional partition 
# MAGIC predicate using WHERE:
# MAGIC
# MAGIC ###### SQL
# MAGIC
# MAGIC     optimize tableName where date>='01-01-2023';
# MAGIC     optimize tableFormat.`tablePath` where date>='01-01-2023';
# MAGIC
# MAGIC ###### Python
# MAGIC
# MAGIC     DeltaTable.forName(spark,'tableName').optimize().where("date>='01-01-2023'").executeCompaction()
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize people_ds_sql

# COMMAND ----------

# MAGIC %md
# MAGIC #### Z-order by columns (Data skipping with Z-order indexes for Delta Lake)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize people_ds_sql 
# MAGIC zorder by (gender);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean up snapshots with VACUUM
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Vacuum syntax
# MAGIC
# MAGIC     VACUUM eventsTable   -- vacuum files not required by versions older than the default retention period
# MAGIC
# MAGIC     VACUUM '/data/events' -- vacuum files in path-based table
# MAGIC
# MAGIC     VACUUM delta.`/data/events/`
# MAGIC
# MAGIC     VACUUM delta.`/data/events/` RETAIN 100 HOURS  -- vacuum files not required by versions more than 100 hours old
# MAGIC
# MAGIC     VACUUM eventsTable DRY RUN    -- do dry run to get the list of files to be deleted

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Purge metadata-only deletes to force data rewrite
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum people_ds_sql
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column Mapping (Rename and drop columns)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### How to enable Delta Lake column mapping
# MAGIC
# MAGIC
# MAGIC         ALTER TABLE <table-name> SET TBLPROPERTIES (
# MAGIC           'delta.minReaderVersion' = '2',
# MAGIC           'delta.minWriterVersion' = '5',
# MAGIC           'delta.columnMapping.mode' = 'name'
# MAGIC         )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Rename a column
# MAGIC When column mapping is enabled for a Delta table, you can rename a column:
# MAGIC
# MAGIC     ALTER TABLE <table-name> RENAME COLUMN old_col_name TO new_col_name
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Drop columns
# MAGIC
# MAGIC When column mapping is enabled for a Delta table, you can drop one or more columns:
# MAGIC
# MAGIC             ALTER TABLE table_name DROP COLUMN col_name;
# MAGIC             ALTER TABLE table_name DROP COLUMNS (col_name_1, col_name_2, ...)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Streaming with column mapping and schema changes
# MAGIC
# MAGIC
# MAGIC     checkpoint_path = "/path/to/checkpointLocation"
# MAGIC     spark.readStream
# MAGIC       .option("schemaTrackingLocation", checkpoint_path)
# MAGIC       .table("delta_source_table")
# MAGIC       .writeStream
# MAGIC       .option("checkpointLocation", checkpoint_path)
# MAGIC       .toTable("output_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Constraints
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Enforced constraints on Databricks
# MAGIC
# MAGIC 1. NOT NULL
# MAGIC 1. CHECK
# MAGIC
# MAGIC
# MAGIC
# MAGIC **NOT NULL constraint**
# MAGIC
# MAGIC
# MAGIC
# MAGIC     CREATE TABLE people10m (
# MAGIC       id INT NOT NULL,
# MAGIC       middleName STRING NOT NULL,
# MAGIC       ssn STRING
# MAGIC     ) USING DELTA;
# MAGIC
# MAGIC     ALTER TABLE people10m ALTER COLUMN middleName DROP NOT NULL;
# MAGIC     ALTER TABLE people10m ALTER COLUMN ssn SET NOT NULL;
# MAGIC
# MAGIC If you specify a NOT NULL constraint on a column nested within a struct, the parent struct must also be not null. Columns nested within array or map types do not accept NOT NULL constraints.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. CHECK constraint
# MAGIC
# MAGIC
# MAGIC     ALTER TABLE people10m ADD CONSTRAINT dateWithinRange CHECK (birthDate > '1900-01-01');
# MAGIC     ALTER TABLE people10m DROP CONSTRAINT dateWithinRange;
# MAGIC
# MAGIC CHECK constraints are exposed as table properties in the output of the DESCRIBE DETAIL and SHOW TBLPROPERTIES commands.
# MAGIC
# MAGIC     ALTER TABLE people10m ADD CONSTRAINT validIds CHECK (id > 1 and id < 99999999);
# MAGIC
# MAGIC     DESCRIBE DETAIL people10m;
# MAGIC     SHOW TBLPROPERTIES people10m;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Declare primary key and foreign key relationships
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC     CREATE TABLE T(pk1 INTEGER NOT NULL, pk2 INTEGER NOT NULL,
# MAGIC     CONSTRAINT t_pk PRIMARY KEY(pk1, pk2));
# MAGIC     CREATE TABLE S(pk INTEGER NOT NULL PRIMARY KEY,fk1 INTEGER,
# MAGIC     fk2 INTEGER,CONSTRAINT s_t_fk FOREIGN KEY(fk1, fk2) REFERENCES T);
# MAGIC
# MAGIC You can query the information_schema or use DESCRIBE to get details about how constraints are applied across a given catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enrich Delta Lake tables with custom metadata
# MAGIC
# MAGIC
# MAGIC ##### Set user-defined commit metadata
# MAGIC
# MAGIC ###### SQL
# MAGIC     SET spark.databricks.delta.commitInfo.userMetadata=overwritten-for-fixing-incorrect-data
# MAGIC     INSERT OVERWRITE default.people10m SELECT * FROM morePeople
# MAGIC
# MAGIC ###### Python
# MAGIC
# MAGIC     df.write.format("delta").mode("overwrite").option("userMetadata", "overwritten-for-fixing-incorrect-data") 
# MAGIC     .save("/tmp/delta/people10m")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Store custom tags in table properties
# MAGIC
# MAGIC     ALTER TABLE default.people10m SET TBLPROPERTIES ('department' = 'accounting', 'delta.appendOnly' = 'true');
# MAGIC     -- Show the table's properties.
# MAGIC     SHOW TBLPROPERTIES default.people10m;
# MAGIC     -- Show just the 'department' table property.
# MAGIC     SHOW TBLPROPERTIES default.people10m ('department');
# MAGIC     
# MAGIC TBLPROPERTIES are stored as part of Delta table metadata. You cannot define new TBLPROPERTIES in a CREATE statement if a Delta table already exists in a given location.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generated columns
# MAGIC
# MAGIC **SQL**
# MAGIC
# MAGIC     CREATE TABLE default.people10m (id INT,dateOfBirth DATE GENERATED ALWAYS AS (CAST(birthDate AS DATE)))
# MAGIC
# MAGIC **Python**
# MAGIC
# MAGIC     DeltaTable.create(spark).tableName("default.people10m").addColumn("id", "INT") \
# MAGIC       .addColumn("dateOfBirth", DateType(), generatedAlwaysAs="CAST(birthDate AS DATE)") \
# MAGIC       .execute()
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC     CREATE TABLE events(eventId BIGINT,eventDate date GENERATED ALWAYS AS (CAST(eventTime AS DATE)))
# MAGIC     PARTITIONED BY (eventType, eventDate)
# MAGIC
# MAGIC If you then run the following query:
# MAGIC
# MAGIC     SELECT * FROM events WHERE eventTime >= "2020-10-01 00:00:00" <= "2020-10-01 12:00:00"
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC As another example, given the following table:
# MAGIC
# MAGIC     CREATE TABLE events(
# MAGIC     eventId BIGINT,
# MAGIC     year INT GENERATED ALWAYS AS (YEAR(eventTime)),
# MAGIC     month INT GENERATED ALWAYS AS (MONTH(eventTime)),
# MAGIC     day INT GENERATED ALWAYS AS (DAY(eventTime))
# MAGIC     )
# MAGIC     PARTITIONED BY (eventType, year, month, day)
# MAGIC
# MAGIC If you then run the following query:
# MAGIC
# MAGIC     SELECT * FROM events WHERE eventTime >= "2020-10-01 00:00:00" <= "2020-10-01 12:00:00"
# MAGIC
# MAGIC Delta Lake automatically generates a partition filter so that the preceding query only reads the data in partition year=2020/month=10/day=01 even if a partition filter is not specified.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deletion vectors
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Enable deletion vectors
# MAGIC
# MAGIC
# MAGIC     CREATE TABLE <table-name> [options] TBLPROPERTIES ('delta.enableDeletionVectors' = true);
# MAGIC
# MAGIC     ALTER TABLE <table-name> SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Apply changes to Parquet data files
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta Lake schema validation
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta table properties
# MAGIC
# MAGIC
# MAGIC Delta Lake conf:
# MAGIC
# MAGIC     delta.<conf>
# MAGIC
# MAGIC SparkSession conf:
# MAGIC
# MAGIC     spark.databricks.delta.properties.defaults.<conf>
# MAGIC
# MAGIC For example, to set the delta.appendOnly = true property for all new Delta Lake tables created in a session, set the following:
# MAGIC
# MAGIC
# MAGIC     SET spark.databricks.delta.properties.defaults.appendOnly = true
# MAGIC
# MAGIC To modify table properties of existing tables, use SET TBLPROPERTIES.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### delta.appendOnly
# MAGIC
# MAGIC true for this Delta table to be append-only. If append-only, existing records cannot be deleted, and existing values cannot be updated.
# MAGIC
# MAGIC Default: false

# COMMAND ----------

# MAGIC %md
# MAGIC ##### delta.autoOptimize.autoCompact
# MAGIC
# MAGIC auto for Delta Lake to automatically optimize the layout of the files for this Delta table.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### delta.autoOptimize.optimizeWrite
# MAGIC
# MAGIC true for Delta Lake to automatically optimize the layout of the files for this Delta table during writes.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### delta.columnMapping.mode
# MAGIC
# MAGIC Whether column mapping is enabled for Delta table columns and the corresponding Parquet columns that use different names. Enabling delta.columnMapping.mode automatically enables delta.randomFilePrefixes.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### delta.enableChangeDataFeed
# MAGIC
# MAGIC true to enable change data feed.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### delta.isolationLevel
# MAGIC
# MAGIC The degree to which a transaction must be isolated from modifications made by concurrent transactions. Valid values are Serializable and WriteSerializable.
# MAGIC
# MAGIC Default: WriteSerializable

# COMMAND ----------

# MAGIC %md
# MAGIC ##### delta.logRetentionDuration
# MAGIC
# MAGIC How long the history for a Delta table is kept. VACUUM operations override this retention threshold.
# MAGIC
# MAGIC Each time a checkpoint is written, Delta Lake automatically cleans up log entries older than the retention interval. If you set this property to a large enough value, many log entries are retained. This should not impact performance as operations against the log are constant time. Operations on history are parallel but will become more expensive as the log size increases.
# MAGIC
# MAGIC Data type: CalendarInterval
# MAGIC
# MAGIC Default: interval 30 days

# COMMAND ----------

# MAGIC %md
# MAGIC ##### delta.logRetentionDuration
# MAGIC
# MAGIC How long the history for a Delta table is kept. VACUUM operations override this retention threshold.
# MAGIC
# MAGIC Each time a checkpoint is written, Delta Lake automatically cleans up log entries older than the retention interval. If you set this property to a large enough value, many log entries are retained. This should not impact performance as operations against the log are constant time. Operations on history are parallel but will become more expensive as the log size increases.
# MAGIC
# MAGIC Data type: CalendarInterval
# MAGIC
# MAGIC Default: interval 30 days

# COMMAND ----------

# MAGIC %md
# MAGIC ##### delta.deletedFileRetentionDuration
# MAGIC
# MAGIC The shortest duration for Delta Lake to keep logically deleted data files before deleting them physically. This is to prevent failures in stale readers after compactions or partition overwrites. This value should be large enough to ensure that:
# MAGIC
# MAGIC It is larger than the longest possible duration of a job if you run VACUUM when there are concurrent readers or writers accessing the Delta table.
# MAGIC
# MAGIC If you run a streaming query that reads from the table, that query does not stop for longer than this value. Otherwise, the query may not be able to restart, as it must still read old files.
# MAGIC
# MAGIC Data type: CalendarInterval
# MAGIC
# MAGIC Default: interval 1 week

# COMMAND ----------

# MAGIC %md
# MAGIC ##### delta.tuneFileSizesForRewrites
# MAGIC
# MAGIC true to always use lower file sizes for all data layout optimization operations on the Delta table. false to never tune to lower file sizes, that is, prevent auto-detection from being activated.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### delta.dataSkippingNumIndexedCols
# MAGIC
# MAGIC The number of columns for Delta Lake to collect statistics about for data skipping. A value of -1 means to collect statistics for all columns. Updating this property does not automatically collect statistics again; instead, it redefines the statistics schema of the Delta table. Specifically, it changes the behavior of future statistics collection (such as during appends and optimizations) as well as data skipping (such as ignoring column statistics beyond this number, even when such statistics exist).
# MAGIC
# MAGIC Default: 32

# COMMAND ----------

# MAGIC %md
# MAGIC ##### delta.setTransactionRetentionDuration
# MAGIC
# MAGIC The shortest duration within which new snapshots will retain transaction identifiers (for example, SetTransactions). When a new snapshot sees a transaction identifier older than or equal to the duration specified by this property, the snapshot considers it expired and ignores it. The SetTransaction identifier is used when making the writes idempotent. See Idempotent table writes in foreachBatch for details.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### delta.targetFileSize
# MAGIC
# MAGIC The target file size in bytes or higher units for file tuning. For example, 104857600 (bytes) or 100mb.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update Delta Lake table schema
# MAGIC
# MAGIC Delta Lake lets you update the schema of a table. The following types of changes are supported:
# MAGIC
# MAGIC 1. Adding new columns (at arbitrary positions)
# MAGIC 1. Reordering existing columns
# MAGIC 1. Renaming existing columns
# MAGIC
# MAGIC You can make these changes explicitly using DDL or implicitly using DML.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Explicitly update schema to add columns
# MAGIC
# MAGIC     ALTER TABLE table_name ADD COLUMNS (col_name data_type [COMMENT col_comment] [FIRST|AFTER colA_name], ...)
# MAGIC
# MAGIC By default, nullability is true. To add a column to a nested field, use:
# MAGIC
# MAGIC     ALTER TABLE table_name ADD COLUMNS (col_name.nested_col_name data_type [COMMENT col_comment] [FIRST|AFTER colA_name], ...)
# MAGIC
# MAGIC For example, if the schema before running ALTER TABLE boxes ADD COLUMNS (colB.nested STRING AFTER field1) is:
# MAGIC
# MAGIC     - root
# MAGIC     | - colA
# MAGIC     | - colB
# MAGIC     | +-field1
# MAGIC     | +-field2
# MAGIC
# MAGIC the schema after is:
# MAGIC
# MAGIC
# MAGIC     - root
# MAGIC     | - colA
# MAGIC     | - colB
# MAGIC     | +-field1
# MAGIC     | +-nested
# MAGIC     | +-field2
# MAGIC
# MAGIC Adding nested columns is supported only for structs. Arrays and maps are not supported.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Explicitly update schema to change column comment or ordering
# MAGIC
# MAGIC     ALTER TABLE table_name ALTER [COLUMN] col_name (COMMENT col_comment | FIRST | AFTER colA_name)
# MAGIC
# MAGIC To change a column in a nested field, use:
# MAGIC
# MAGIC     ALTER TABLE table_name ALTER [COLUMN] col_name.nested_col_name (COMMENT col_comment | FIRST | AFTER colA_name)
# MAGIC
# MAGIC For example, if the schema before running ALTER TABLE boxes ALTER COLUMN colB.field2 FIRST is:
# MAGIC
# MAGIC
# MAGIC     - root
# MAGIC     | - colA
# MAGIC     | - colB
# MAGIC     | +-field1
# MAGIC     | +-field2
# MAGIC the schema after is:
# MAGIC
# MAGIC     - root
# MAGIC     | - colA
# MAGIC     | - colB
# MAGIC     | +-field2
# MAGIC     | +-field1
# MAGIC     

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Explicitly update schema to replace columns
# MAGIC
# MAGIC     ALTER TABLE table_name REPLACE COLUMNS (col_name1 col_type1 [COMMENT col_comment1], ...)
# MAGIC
# MAGIC For example, when running the following DDL:
# MAGIC
# MAGIC     ALTER TABLE boxes REPLACE COLUMNS (colC STRING, colB STRUCT<field2:STRING, nested:STRING, field1:STRING>, colA STRING)
# MAGIC
# MAGIC if the schema before is:
# MAGIC
# MAGIC     - root
# MAGIC     | - colA
# MAGIC     | - colB
# MAGIC     | +-field1
# MAGIC     | +-field2
# MAGIC the schema after is:
# MAGIC
# MAGIC     - root
# MAGIC     | - colC
# MAGIC     | - colB
# MAGIC     | +-field2
# MAGIC     | +-nested
# MAGIC     | +-field1
# MAGIC     | - colA

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Explicitly update schema to rename columns
# MAGIC
# MAGIC To rename columns without rewriting any of the columns’ existing data, you must enable column mapping for the table.
# MAGIC
# MAGIC ###### To rename a column:
# MAGIC
# MAGIC     ALTER TABLE table_name RENAME COLUMN old_col_name TO new_col_name
# MAGIC
# MAGIC To rename a nested field:
# MAGIC
# MAGIC     ALTER TABLE table_name RENAME COLUMN col_name.old_nested_field TO new_nested_field
# MAGIC
# MAGIC For example, when you run the following command:
# MAGIC
# MAGIC     ALTER TABLE boxes RENAME COLUMN colB.field1 TO field001
# MAGIC
# MAGIC If the schema before is:
# MAGIC
# MAGIC     - root
# MAGIC     | - colA
# MAGIC     | - colB
# MAGIC     | +-field1
# MAGIC     | +-field2
# MAGIC
# MAGIC Then the schema after is:
# MAGIC
# MAGIC     - root
# MAGIC     | - colA
# MAGIC     | - colB
# MAGIC     | +-field001
# MAGIC     | +-field2

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Explicitly update schema to drop columns
# MAGIC
# MAGIC
# MAGIC To drop columns as a metadata-only operation without rewriting any data files, you must enable column mapping for the table. 
# MAGIC Dropping a column from metadata does not delete the underlying data for the column in files. To purge the dropped column data, you can use REORG TABLE to rewrite files. You can then use VACUUM to physically delete the files that contain the dropped column data.
# MAGIC
# MAGIC To drop a column:
# MAGIC
# MAGIC     ALTER TABLE table_name DROP COLUMN col_name
# MAGIC To drop multiple columns:
# MAGIC
# MAGIC     ALTER TABLE table_name DROP COLUMNS (col_name_1, col_name_2)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Explicitly update schema to change column type or name
# MAGIC
# MAGIC You can change a column’s type or name or drop a column by rewriting the table. To do this, use the overwriteSchema option.
# MAGIC
# MAGIC The following example shows changing a column type:
# MAGIC
# MAGIC     spark.read.table(...).withColumn("birthDate", col("birthDate").cast("date"))
# MAGIC     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(...)
# MAGIC   
# MAGIC The following example shows changing a column name:
# MAGIC
# MAGIC     spark.read.table(...).withColumnRenamed("dateOfBirth", "birthDate")
# MAGIC     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(...)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Add columns with automatic schema update
# MAGIC
# MAGIC Columns that are present in the DataFrame but missing from the table are automatically added as part of a write transaction when:
# MAGIC
# MAGIC 1. write or writeStream have .option("mergeSchema", "true")
# MAGIC 1. spark.databricks.delta.schema.autoMerge.enabled is true
# MAGIC
# MAGIC When both options are specified, the option from the DataFrameWriter takes precedence. The added columns are appended to the end of the struct they are present in. Case is preserved when appending a new column.
# MAGIC
# MAGIC mergeSchema cannot be used with INSERT INTO or .write.insertInto().

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Automatic schema evolution for Delta Lake merge
# MAGIC
# MAGIC Schema evolution allows users to resolve schema mismatches between the target and source table in merge. It handles the following two cases:
# MAGIC
# MAGIC 1. A column in the source table is not present in the target table. The new column is added to the target schema, and its values are inserted or updated using the source values.
# MAGIC
# MAGIC 1. A column in the target table is not present in the source table. The target schema is left unchanged; the values in the additional target column are either left unchanged (for UPDATE) or set to NULL (for INSERT).
# MAGIC
# MAGIC To use schema evolution, you must set the Spark session configuration`spark.databricks.delta.schema.autoMerge.enabled` to true before you run the merge command.
# MAGIC
# MAGIC In Databricks Runtime 12.2 and above, columns present in the source table can be specified by name in insert or update actions. In Databricks Runtime 12.1 and below, only INSERT * or UPDATE SET * actions can be used for schema evolution with merge.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC | Scenerio | SQL  | Behavior without schema evolution (default)| Behavior with schema evolution |
# MAGIC | ----------- | ----------- |--------------------------------- |--------------------------------- |
# MAGIC | **Target columns**: key, value **Source columns**: key, value, new_value| <span style="color:red"> MERGE INTO target_table t USING source_table s ON t.key = s.key  WHEN MATCHED THEN UPDATE SET *  WHEN NOT MATCHED THEN INSERT * </span>| The table schema remains unchanged; only columns key, value are updated/inserted.| The table schema is changed to (key, value, new_value). Existing records with matches are updated with the value and new_value in the source. New rows are inserted with the schema (key, value, new_value).|
# MAGIC |**Target columns:** key, old_value   **Source columns:** key, new_value| <span style="color:red">MERGE INTO target_table t USING source_table s  ON t.key = s.key  WHEN MATCHED  THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT * </span>| UPDATE and INSERT actions throw an error because the target column old_value is not in the source.|The table schema is changed to (key, old_value, new_value). Existing records with matches are updated with the new_value in the source leaving old_value unchanged. New records are inserted with the specified key, new_value, and NULL for the old_value.|
# MAGIC | **Target columns:** key, old_value  **Source columns:** key, new_value| <span style="color:red"> MERGE INTO target_table t  USING source_table s  ON t.key = s.key WHEN MATCHED  THEN UPDATE SET new_value = s.new_value </span> |UPDATE throws an error because column new_value does not exist in the target table.| The table schema is changed to (key, old_value, new_value). Existing records with matches are updated with the new_value in the source leaving old_value unchanged, and unmatched records have NULL entered for new_value. |
# MAGIC | **Target columns:** key, old_value **Source columns:** key, new_value|<span style="color:red"> MERGE INTO target_table t USING source_table s  ON t.key = s.key  WHEN NOT MATCHED THEN INSERT (key, new_value) VALUES (s.key, s.new_value) |INSERT throws an error because column new_value does not exist in the target table.| The table schema is changed to (key, old_value, new_value). New records are inserted with the specified key, new_value, and NULL for the old_value. Existing records have NULL entered for new_value leaving old_value unchanged. |

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Exclude columns with Delta Lake merge
# MAGIC
# MAGIC You can use EXCEPT clauses in merge conditions to explicitly exclude columns. The behavior of the EXCEPT keyword varies depending on whether or not schema evolution is enabled. 
# MAGIC
# MAGIC 1. With schema evolution disabled, the EXCEPT keyword applies to the list of columns in the target table and allows excluding columns from UPDATE or INSERT actions. Excluded columns are set to null.
# MAGIC
# MAGIC 1. With schema evolution enabled, the EXCEPT keyword applies to the list of columns in the source table and allows excluding columns from schema evolution. A new column in the source that is not present in the target is not added to the target schema if it is listed in the EXCEPT clause. Excluded columns that are already present in the target are set to null.
# MAGIC
# MAGIC The following examples demonstrate this syntax:

# COMMAND ----------

# MAGIC %md
# MAGIC | Scenerio | SQL  | Behavior without schema evolution (default)| Behavior with schema evolution |
# MAGIC | ----------- | ----------- |--------------------------------- |--------------------------------- |
# MAGIC | **Target columns**: id, title, last_updated **Source columns**: id, title, review, last_updated | <span style="color:red"> MERGE INTO target_table t USING source_table s ON t.id = s.id WHEN MATCHED THEN UPDATE SET last_updated = current_date() WHEN NOT MATCHED THEN INSERT * EXCEPT (last_updated) </span>|Matched rows are updated by setting the last_updated field to the current date. New rows are inserted using values for id and title. The excluded field last_updated is set to null. The field review is ignored because it is not in the target. | Matched rows are updated by setting the last_updated field to the current date. Schema is evolved to add the field review. New rows are inserted using all source fields except last_updated which is set to null.|
# MAGIC |**Target columns:** id, title, last_updated   **Source columns:** id, title, review, internal_count | <span style="color:red">MERGE INTO target_table t USING source_table s  ON t.id = s.id  WHEN MATCHED THEN UPDATE SET last_updated = current_date() WHEN NOT MATCHED THEN INSERT * EXCEPT (last_updated, internal_count)</span>| INSERT throws an error because column internal_count does not exist in the target table.| Matched rows are updated by setting the last_updated field to the current date. The review field is added to the target table, but the internal_count field is ignored. New rows inserted have last_updated set to null.|
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Automatic schema evolution for arrays of structs
# MAGIC Delta MERGE INTO supports resolving struct fields by name and evolving schemas for arrays of structs. With schema evolution enabled, target table schemas will evolve for arrays of structs, which also works with any nested structs inside of arrays. Here are a few examples of the effects of merge operations with and without schema evolution for arrays of structs.

# COMMAND ----------

# MAGIC %md
# MAGIC | Source schema | Target schema  | Behavior without schema evolution (default)| Behavior with schema evolution |
# MAGIC | ----------- | ----------- |--------------------------------- |--------------------------------- |
# MAGIC | array<struct<b: string, a: string>> | array<struct<a: int, b: int>> | The table schema remains unchanged. Columns will be resolved by name and updated or inserted.|The table schema remains unchanged. Columns will be resolved by name and updated or inserted. |
# MAGIC | array<struct<a: int, c: string, d: string>> |array<struct<a: string, b: string>> |update and insert throw errors because c and d do not exist in the target table. | The table schema is changed to array<struct<a: string, b: string, c: string, d: string>>. c and d are inserted as NULL for existing entries in the target table. update and insert fill entries in the source table with a casted to string and b as NULL.|
# MAGIC | array<struct<a: string, b: struct<c: string, d: string>>> |array<struct<a: string, b: struct<c: string>>> | update and insert throw errors because d does not exist in the target table.| The target table schema is changed to array<struct<a: string, b: struct<c: string, d: string>>>. d is inserted as NULL for existing entries in the target table.|
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Dealing with NullType columns in schema updates
# MAGIC
# MAGIC Because Parquet doesn’t support NullType, NullType columns are dropped from the DataFrame when writing into Delta tables, but are still stored in the schema. When a different data type is received for that column, Delta Lake merges the schema to the new data type. If Delta Lake receives a NullType for an existing column, the old schema is retained and the new column is dropped during the write.
# MAGIC
# MAGIC NullType in streaming is not supported. Since you must set schemas when using streaming this should be very rare. NullType is also not accepted for complex types such as ArrayType and MapType.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Replace table schema
# MAGIC By default, overwriting the data in a table does not overwrite the schema. When overwriting a table using mode("overwrite") without replaceWhere, you may still want to overwrite the schema of the data being written. You replace the schema and partitioning of the table by setting the overwriteSchema option to true:
# MAGIC
# MAGIC     df.write.option("overwriteSchema", "true")
# MAGIC
# MAGIC You cannot specify overwriteSchema as true when using dynamic partition overwrite.
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Partition tables
# MAGIC
# MAGIC This article provides an overview of how you can partition tables on Databricks and specific recommendations around when you should use partitioning for tables backed by Delta Lake. Because of built-in features and optimizations, most tables with less than 1 TB of data do not require partitions.
# MAGIC
# MAGIC 1. Databricks automatically clusters data in unpartitioned tables by ingestion time. Ingestion time provides similar query benefits to partitioning strategies based on datetime fields without any need to optimize or tune your data. To maintain ingestion time clustering when you perform a large number of modifications using UPDATE or MERGE statements on a table, Databricks recommends running OPTIMIZE with ZORDER BY using a column that matches the ingestion order. For instance, this could be a column containing an event timestamp or a creation date.
# MAGIC
# MAGIC 1. Databricks recommends you do not partition tables that contains less than a terabyte of data.
# MAGIC
# MAGIC 1. Databricks recommends all partitions contain at least a gigabyte of data. Tables with fewer, larger partitions tend to outperform tables with many smaller partitions.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Do Delta Lake and Parquet share partitioning strategies?
# MAGIC Delta Lake uses Parquet as the primary format for storing data, and some Delta tables with partitions specified demonstrate organization similar to Parquet tables stored with Apache Spark. Apache Spark uses Hive-style partitioning when saving data in Parquet format. Hive-style partitioning is not part of the Delta Lake protocol, and workloads should not rely on this partitioning strategy to interact with Delta tables.
# MAGIC
# MAGIC Many Delta Lake features break assumptions about data layout that might have been transferred from Parquet, Hive, or even earlier Delta Lake protocol versions. You should always interact with data stored in Delta Lake using officially supported clients and APIs.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### How are Delta Lake partitions different from partitions in other data lakes?
# MAGIC f you do choose to partition your table, consider the following facts before choosing a strategy:
# MAGIC
# MAGIC 1. Transactions are not defined by partition boundaries. Delta Lake ensures ACID through transaction logs, so you do not need to separate a batch of data by a partition to ensure atomic discovery.
# MAGIC
# MAGIC 1. Databricks compute clusters do not have data locality tied to physical media. Data ingested into the lakehouse is stored in cloud object storage. While data is cached to local disk storage during data processing, Databricks uses file-based statistics to identify the minimal amount of data for parallel loading.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### How do Z-order and partitions work together?
# MAGIC You can use Z-order indexes alongside partitions to speed up queries on large datasets. Most tables can leverage ingestion time clustering to avoid needing to worry about Z-order and partition tuning.
# MAGIC
# MAGIC The following rules are important to keep in mind while planning a query optimization strategy based on partition boundaries and Z-order:
# MAGIC
# MAGIC 1. Z-order works in tandem with the OPTIMIZE command. You cannot combine files across partition boundaries, and so Z-order clustering can only occur within a partition. For unpartitioned tables, files can be combined across the entire table.
# MAGIC
# MAGIC 1. Partitioning works well only for low or known cardinality fields (for example, date fields or physical locations), but not for fields with high cardinality such as timestamps. Z-order works for all fields, including high cardinality fields and fields that may grow infinitely (for example, timestamps or the customer ID in a transactions or orders table).
# MAGIC
# MAGIC You cannot Z-order on fields used for partitioning.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### If partitions are so bad, why do some Databricks features use them?
# MAGIC Partitions can be beneficial, especially for very large tables. Many performance enhancements around partitioning focus on very large tables (hundreds of terabytes or greater).
# MAGIC
# MAGIC Many customers migrate to Delta Lake from Parquet-based data lakes. The CONVERT TO DELTA statement allows you to convert an existing Parquet-based table to a Delta table without rewriting existing data. As such, many customers have large tables that inherit previous partitioning strategies. Some optimizations developed by Databricks seek to leverage these partitions when possible, mitigating some potential downsides for partitioning strategies not optimized for Delta Lake.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copy INTO
# MAGIC
# MAGIC COPY INTO SQL command lets you load data from a file location into a Delta table. This is a re-triable and idempotent operation; files in the source location that have already been loaded are skipped.
# MAGIC
# MAGIC COPY INTO must target an existing Delta table. In Databricks Runtime 11.0 and above, setting the schema for these tables is optional for formats that support schema evolution.
# MAGIC
# MAGIC Databricks recommends that you use the COPY INTO command for incremental and bulk data loading for data sources that contain thousands of files. Databricks recommends that you use Auto Loader for advanced use cases.
# MAGIC
# MAGIC For a more scalable and robust file ingestion experience, Databricks recommends that SQL users leverage streaming tables. See Load data using streaming tables in Databricks SQL.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Supported source formats
# MAGIC Supported source formats for COPY INTO include CSV, JSON, Avro, ORC, Parquet, text, and binary files. The source can be anywhere that your Databricks workspace has access to.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example: Load data into a schemaless Delta Lake table
# MAGIC
# MAGIC You can create empty placeholder Delta tables so that the schema is later inferred during a COPY INTO command by setting mergeSchema to true in COPY_OPTIONS:
# MAGIC
# MAGIC     CREATE TABLE IF NOT EXISTS my_table
# MAGIC     [COMMENT <table-description>]
# MAGIC     [TBLPROPERTIES (<table-properties>)];
# MAGIC
# MAGIC     COPY INTO my_table
# MAGIC     FROM '/path/to/files'
# MAGIC     FILEFORMAT = <format>
# MAGIC     FORMAT_OPTIONS ('mergeSchema' = 'true')
# MAGIC     COPY_OPTIONS ('mergeSchema' = 'true');
# MAGIC
# MAGIC SQL statement above is idempotent and can be scheduled to run to ingest data exactly-once into a Delta table. The empty Delta table is not usable outside of COPY INTO. INSERT INTO and MERGE INTO are not supported to write data into schemaless Delta tables. After data is inserted into the table with COPY INTO, the table becomes queryable.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example: Load data into a Delta Lake table
# MAGIC
# MAGIC **SQL**
# MAGIC
# MAGIC     COPY INTO default.loan_risks_upload
# MAGIC     FROM '/databricks-datasets/learning-spark-v2/loans/loan-risks.snappy.parquet'
# MAGIC     FILEFORMAT = PARQUET;
# MAGIC
# MAGIC **Python**
# MAGIC
# MAGIC     table_name = 'default.loan_risks_upload'
# MAGIC     source_data = '/databricks-datasets/learning-spark-v2/loans/loan-risks.snappy.parquet'
# MAGIC     source_format = 'PARQUET'
# MAGIC     spark.sql("COPY INTO " + table_name + " FROM '" + source_data + "'" + " FILEFORMAT = " + source_format)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configure data access for ingestion
# MAGIC
# MAGIC This article describes the following ways to configure secure access to source data:
# MAGIC
# MAGIC 1. (Recommended) Create a Unity Catalog volume (READ VOLUME privilege on the volume).
# MAGIC
# MAGIC 1. Create a Unity Catalog external location with a storage credential (READ FILES privilege on the external location)
# MAGIC
# MAGIC 1. Launch a compute resource that uses service principal (Databricks workspace admin permissions)
# MAGIC
# MAGIC 1. Generate temporary credentials (access key ID, a secret key, and a session token) to share with other Databricks users.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load data using COPY INTO with Unity Catalog volumes or external locations
# MAGIC
# MAGIC Unity Catalog adds new options for configuring secure access to raw data. You can use Unity Catalog volumes or external locations to access data in cloud object storage.
# MAGIC
# MAGIC Databricks recommends using volumes to access files in cloud storage as part of the ingestion process using COPY INTO. 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pre-reqs
# MAGIC
# MAGIC 1. READ VOLUME privilege on a volume or the READ FILES privilege on an external location.
# MAGIC
# MAGIC 1. The path to your source data in the form of a cloud object storage URL or a volume path. 
# MAGIC
# MAGIC     Example cloud object storage URLs: s3://landing-bucket/raw-data/json, abfss://container@storageAccount.dfs.core.windows.net/jsonData.
# MAGIC
# MAGIC     Example volume path: /Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/raw_data/json.
# MAGIC
# MAGIC 1. The USE SCHEMA privilege on the schema that contains the target table.
# MAGIC
# MAGIC 1. The USE CATALOG privilege on the parent catalog.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load data from a volume
# MAGIC
# MAGIC To load data from a Unity Catalog volume, you must have the READ VOLUME privilege. Volume privileges apply to all nested directories under the specified volume. For example, if you have access to a volume with the path /Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/, the following commands are valid:
# MAGIC
# MAGIC
# MAGIC     COPY INTO landing_table
# MAGIC     FROM '/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/raw_data'
# MAGIC     FILEFORMAT = PARQUET;
# MAGIC
# MAGIC Optionally, you can also use a volume path with the dbfs scheme. For example, the following commands are also valid:
# MAGIC
# MAGIC     COPY INTO landing_table
# MAGIC     FROM 'dbfs:/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/raw_data'
# MAGIC     FILEFORMAT = PARQUET;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load data using an external location
# MAGIC
# MAGIC The following example loads data from S3 and ADLS Gen2 into a table using Unity Catalog external locations to provide access to the source code.
# MAGIC
# MAGIC     COPY INTO my_json_data
# MAGIC     FROM 's3://landing-bucket/json-data'
# MAGIC     FILEFORMAT = JSON;
# MAGIC
# MAGIC     COPY INTO my_json_data
# MAGIC     FROM 'abfss://container@storageAccount.dfs.core.windows.net/jsonData'
# MAGIC     FILEFORMAT = JSON;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### External location privilege inheritance
# MAGIC
# MAGIC External location privileges apply to all nested directories under the specified location.For example, if you have access to an external location defined with the URL s3://landing-bucket/raw-data, the following commands are valid:
# MAGIC
# MAGIC         COPY INTO landing_table
# MAGIC         FROM 's3://landing-bucket/raw-data'
# MAGIC         FILEFORMAT = PARQUET;
# MAGIC
# MAGIC         COPY INTO json_table
# MAGIC         FROM 's3://landing-bucket/raw-data/json'
# MAGIC         FILEFORMAT = JSON;

# COMMAND ----------

# MAGIC %md
# MAGIC Permissions on this external location do not grant any privileges on directories above or parallel to the location specified. For example, neither of the following commands are valid:
# MAGIC
# MAGIC     COPY INTO parent_table
# MAGIC     FROM 's3://landing-bucket'
# MAGIC     FILEFORMAT = PARQUET;
# MAGIC
# MAGIC     COPY INTO sibling_table
# MAGIC     FROM 's3://landing-bucket/json-data'
# MAGIC     FILEFORMAT = JSON;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common Design patterns

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Create target tables for COPY INTO
# MAGIC
# MAGIC COPY INTO must target an existing Delta table. In Databricks Runtime 11.0 and above, setting the schema for these tables is optional for formats that support schema evolution:
# MAGIC
# MAGIC     CREATE TABLE IF NOT EXISTS my_table [(col_1 col_1_type, col_2 col_2_type, ...)]
# MAGIC     [COMMENT <table-description>]
# MAGIC     [TBLPROPERTIES (<table-properties>)];
# MAGIC
# MAGIC Note that to infer the schema with COPY INTO, you must pass additional options:
# MAGIC
# MAGIC     COPY INTO my_table
# MAGIC     FROM '/path/to/files'
# MAGIC     FILEFORMAT = <format>
# MAGIC     FORMAT_OPTIONS ('inferSchema' = 'true')
# MAGIC     COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Schemaless table and schemaEvolution
# MAGIC
# MAGIC     CREATE TABLE IF NOT EXISTS my_pipe_data;
# MAGIC
# MAGIC     COPY INTO my_pipe_data
# MAGIC     FROM 's3a://my-bucket/pipeData'
# MAGIC     FILEFORMAT = CSV
# MAGIC     FORMAT_OPTIONS ('mergeSchema' = 'true','delimiter' = '|','header' = 'true')
# MAGIC     COPY_OPTIONS ('mergeSchema' = 'true');
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Load JSON data with COPY INTO
# MAGIC The following example loads JSON data from five files on AWS S3 into the Delta table called my_json_data. This table must be created before COPY INTO can be executed. If any data was already loaded from one of the files, the data isn’t reloaded for that file.
# MAGIC
# MAGIC     COPY INTO my_json_data
# MAGIC     FROM 's3://my-bucket/jsonData'
# MAGIC     FILEFORMAT = JSON
# MAGIC     FILES = ('f1.json', 'f2.json', 'f3.json', 'f4.json', 'f5.json')
# MAGIC
# MAGIC -- The second execution will not copy any data since the first command already loaded the data
# MAGIC
# MAGIC     COPY INTO my_json_data
# MAGIC     FROM 's3://my-bucket/jsonData'
# MAGIC     FILEFORMAT = JSON
# MAGIC     FILES = ('f1.json', 'f2.json', 'f3.json', 'f4.json', 'f5.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Load Avro data with COPY INTO
# MAGIC
# MAGIC The following example loads Avro data on Google Cloud Storage using additional SQL expressions as part of the SELECT statement.
# MAGIC
# MAGIC     COPY INTO my_delta_table
# MAGIC     FROM (SELECT to_date(dt) dt, event as measurement, quantity::double FROM 'gs://my-bucket/avroData')
# MAGIC     FILEFORMAT = AVRO

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Load CSV files with COPY INTO
# MAGIC
# MAGIC The following example loads CSV files from Azure Data Lake Storage Gen2 under abfss://container@storageAccount.dfs.core.windows.net/base/path/f1 into a Delta table at abfss://container@storageAccount.dfs.core.windows.net/deltaTables/target.
# MAGIC
# MAGIC
# MAGIC     COPY INTO delta.`abfss://container@storageAccount.dfs.core.windows.net/deltaTables/target`
# MAGIC     FROM (SELECT key, index, textData, 'constant_value' FROM 'abfss://container@storageAccount.dfs.core.windows.net/base/path')
# MAGIC     FILEFORMAT = CSV
# MAGIC     PATTERN = 'folder1/file_[a-g].csv'
# MAGIC     FORMAT_OPTIONS('header' = 'true')
# MAGIC
# MAGIC The example below loads CSV files without headers on ADLS Gen2 using COPY INTO. By casting the data and renaming the columns, you can put the data in the schema you want
# MAGIC
# MAGIC     COPY INTO delta.`abfss://container@storageAccount.dfs.core.windows.net/deltaTables/target`
# MAGIC     FROM (SELECT _c0::bigint key, _c1::int index, _c2 textData FROM 'abfss://container@storageAccount.dfs.core.windows.net/base/path')
# MAGIC     FILEFORMAT = CSV
# MAGIC     PATTERN = 'folder1/file_[a-g].csv'

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. Ignore corrupt files while loading data
# MAGIC
# MAGIC If the data you’re loading can’t be read due to some corruption issue, those files can be skipped by setting ignoreCorruptFiles to true in the FORMAT_OPTIONS. The result of the COPY INTO command returns how many files were skipped due to corruption in the num_skipped_corrupt_files column. This metric also shows up in the operationMetrics column under numSkippedCorruptFiles after running DESCRIBE HISTORY on the Delta table.
# MAGIC
# MAGIC Corrupt files aren’t tracked by COPY INTO, so they can be reloaded in a subsequent run if the corruption is fixed. You can see which files are corrupt by running COPY INTO in VALIDATE mode.
# MAGIC
# MAGIC     COPY INTO my_table
# MAGIC     FROM '/path/to/files'
# MAGIC     FILEFORMAT = <format>
# MAGIC     [VALIDATE ALL]
# MAGIC     FORMAT_OPTIONS ('ignoreCorruptFiles' = 'true')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1. Configure your environment and create a data generator

# COMMAND ----------

# MAGIC
# MAGIC %python
# MAGIC
# MAGIC username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
# MAGIC database = f"copyinto_{username}_db"
# MAGIC source = f"dbfs:/user/{username}/copy-into-demo"
# MAGIC
# MAGIC spark.sql(f"SET c.username='{username}'")
# MAGIC spark.sql(f"SET c.database={database}")
# MAGIC spark.sql(f"SET c.source='{source}'")
# MAGIC
# MAGIC spark.sql("DROP DATABASE IF EXISTS ${c.database} CASCADE")
# MAGIC spark.sql("CREATE DATABASE ${c.database}")
# MAGIC spark.sql("USE ${c.database}")
# MAGIC
# MAGIC dbutils.fs.rm(source, True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Configure random data generator

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE user_ping_raw
# MAGIC (user_id STRING, ping INTEGER, time TIMESTAMP)
# MAGIC USING json
# MAGIC LOCATION ${c.source};
# MAGIC
# MAGIC CREATE TABLE user_ids (user_id STRING);
# MAGIC
# MAGIC INSERT INTO user_ids VALUES
# MAGIC ("potato_luver"),
# MAGIC ("beanbag_lyfe"),
# MAGIC ("default_username"),
# MAGIC ("the_king"),
# MAGIC ("n00b"),
# MAGIC ("frodo"),
# MAGIC ("data_the_kid"),
# MAGIC ("el_matador"),
# MAGIC ("the_wiz");
# MAGIC
# MAGIC CREATE FUNCTION get_ping()
# MAGIC     RETURNS INT
# MAGIC     RETURN int(rand() * 250);
# MAGIC
# MAGIC CREATE FUNCTION is_active()
# MAGIC     RETURNS BOOLEAN
# MAGIC     RETURN CASE
# MAGIC         WHEN rand() > .25 THEN true
# MAGIC         ELSE false
# MAGIC         END;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Write the sample data to cloud storage
# MAGIC The code provided writes to JSON, simulating an external system that might dump results from another system into object storage.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Write a new batch of data to the data source
# MAGIC INSERT INTO user_ping_raw
# MAGIC SELECT *,
# MAGIC   get_ping() ping,
# MAGIC   current_timestamp() time
# MAGIC FROM user_ids
# MAGIC WHERE is_active()=true;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Use COPY INTO to load JSON data idempotently
# MAGIC
# MAGIC You must create a target Delta Lake table before you can use COPY INTO. In Databricks Runtime 11.0 and above, you do not need to provide anything other than a table name in your CREATE TABLE statement. For previous versions of Databricks Runtime, you must provide a schema when creating an empty table.
# MAGIC
# MAGIC Because this action is idempotent, you can run it multiple times but data will only be loaded once.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create target table and load data
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS user_ping_target;
# MAGIC
# MAGIC COPY INTO user_ping_target
# MAGIC FROM ${c.source}
# MAGIC FILEFORMAT = JSON
# MAGIC FORMAT_OPTIONS ("mergeSchema" = "true")
# MAGIC COPY_OPTIONS ("mergeSchema" = "true")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Load more data and preview results
# MAGIC You can re-run steps 2-3 many times to land new batches of random raw JSON data in your source, idempotently load them to Delta Lake with COPY INTO, and preview the results. Try running these steps out of order or multiple times to simulate multiple batches of raw data being written or executing COPY INTO multiple times without new data having arrived.
# MAGIC
# MAGIC #### Step 5: Clean up tutorial

# COMMAND ----------

# MAGIC %python
# MAGIC # Drop database and tables and remove data
# MAGIC spark.sql("DROP DATABASE IF EXISTS ${c.database} CASCADE")
# MAGIC dbutils.fs.rm(source, True)
# MAGIC

# COMMAND ----------


