# What is TD-Flink
a java project,read from TDengine and write to TDengine on Spark.
# Building 
## Install build dependencies
To install openjdk-8 and maven:
```
sudo apt-get install -y openjdk-8-jdk maven 
```
To install Spark:
```
wget https://www.apache.org/dyn/closer.lua/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz
tar zxf spark-3.2.1-bin-hadoop3.2.tgz -C /usr/local
```
## Build
```
mvn clean package
```
# Run

* run the job
```
spark-submit --master local --name TDenginetest --class com.taosdata.java.SparkTest /testSpark-1.0-SNAPSHOT-dist.jar
```

## IoTDB build and runtime

Build with Java 8 and Maven. Use Spark 3.2.1 for Scala 2.12. The IoTDB JDBC dependency is pinned to a Java 8-compatible version.

## IoTDB Table example

The IoTDB JDBC driver defaults to Tree SQL. Spark's relational JDBC source and
writer need the IoTDB 2.x Table model, so the URL must explicitly include
`sql_dialect=table`. The endpoint uses the IoTDB JDBC RPC port (6667 by default).
`IoTDBSparkDriver` parses that URL parameter exactly, wraps the official JDBC
driver, and adapts Spark's read-only `prepareStatement(sql, type, concurrency)`
call to the overload supported by the IoTDB client. Spark writes BLOBs as binary
values and timestamps using the server's negotiated precision. Successful batches
and failed batches are cleared before the statement is reused. IoTDB has no JDBC
transactions, so each partition writes with `isolationLevel=NONE`. The `dist` JAR
isolates the IoTDB client's Thrift and ANTLR dependencies from Spark's versions.

Create the source and target tables in advance using IoTDB Table SQL. Spark's
generic JDBC writer cannot assign IoTDB's TAG, ATTRIBUTE, FIELD, and TIME column
roles when it creates a missing table. The example appends rows to the target;
choose a bounded `SELECT` query and make its column order and types compatible
with the existing target table. Spark obtains the column metadata with `LIMIT 0`,
which also works on an empty table.

For the example query below, create matching source and target tables:
```sql
CREATE DATABASE example;
CREATE TABLE example.source (
  device_id STRING TAG,
  temperature DOUBLE FIELD
);
CREATE TABLE example.target (
  device_id STRING TAG,
  temperature DOUBLE FIELD
);
```

Set connection details and SQL outside the source tree:
```sh
export IOTDB_JDBC_URL='jdbc:iotdb://localhost:6667?sql_dialect=table'
export IOTDB_USERNAME='your-user'
export IOTDB_PASSWORD='your-password'
export IOTDB_READ_QUERY='SELECT * FROM example.source LIMIT 1000'
export IOTDB_TARGET_TABLE='example.target'
```

Run with Spark 3.2.1:
```sh
spark-submit --master local[*] --class com.taosdata.java.IoTDBSparkTest target/testSpark-1.0-SNAPSHOT-dist.jar
```

### Optional isolated Spark JDBC round-trip

When an IoTDB Table-mode RPC endpoint is available, this opt-in check creates a
randomly named database with two tables, copies two rows through the example's
Spark JDBC query and append path in two batches, and reads the destination back
through Spark. It checks timestamps, tags, doubles, quoted text, binary data and
nulls, then drops its database and verifies that it is gone. Only the three
connection variables are required. It does not run as part of `mvn test`.

```sh
spark-submit --master local[1] --class com.taosdata.java.IoTDBSparkJdbcSmokeTest target/testSpark-1.0-SNAPSHOT-dist.jar
```
