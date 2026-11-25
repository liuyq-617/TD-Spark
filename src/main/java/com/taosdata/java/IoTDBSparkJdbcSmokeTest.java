package com.taosdata.java;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.iotdb.jdbc.IoTDBConnection;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/** Opt-in test of the packaged connector against a Table-model RPC endpoint. */
public final class IoTDBSparkJdbcSmokeTest {
    private IoTDBSparkJdbcSmokeTest() { }

    public static void main(String[] args) throws Exception {
        Map<String, String> options = IoTDBSparkTest.jdbcOptions();
        options.put("batchsize", "1");
        Properties credentials = new Properties();
        credentials.setProperty("user", options.get("user"));
        credentials.setProperty("password", options.get("password"));
        String database = "spark_jdbc_smoke_" + UUID.randomUUID().toString().replace("-", "");
        try (Connection connection = new IoTDBSparkDriver().connect(options.get("url"), credentials);
             Statement sql = connection.createStatement()) {
            check(!connection.getMetaData().supportsTransactions(), "IoTDB transaction metadata");
            sql.execute("CREATE DATABASE " + database);
            try (AutoCloseable cleanup = () -> {
                sql.execute("DROP DATABASE " + database);
                try (ResultSet databases = sql.executeQuery("SHOW DATABASES")) {
                    while (databases.next()) {
                        check(!database.equalsIgnoreCase(databases.getString(1)), "database cleanup");
                    }
                }
                System.out.println("Dropped and verified " + database);
            }) {
                String source = database + ".source_data";
                String target = database + ".target_data";
                String schema = " (device_id STRING TAG, reading DOUBLE FIELD, note STRING FIELD, payload BLOB FIELD)";
                sql.execute("CREATE TABLE " + source + schema);
                sql.execute("CREATE TABLE " + target + schema);
                try (ResultSet empty = sql.executeQuery(new IoTDBDialect().getSchemaQuery(target))) {
                    check(empty.getMetaData().getColumnCount() == 5 && !empty.next(), "empty table schema");
                }
                int ticksPerSecond = ((IoTDBConnection) connection).getTimeFactor();
                long firstMillis = 1700000000123L;
                long firstTick = firstMillis * (ticksPerSecond / 1000);
                String columns = "time, device_id, reading, note, payload";
                sql.execute("INSERT INTO " + source + " (" + columns + ") VALUES (" + firstTick
                        + ", 'sensor_a', 12.5, 'O''Brien', X'00ff'), (" + (firstTick + ticksPerSecond)
                        + ", 'sensor_a', -2.25, NULL, NULL)");
                SparkSession spark = SparkSession.builder().appName("IoTDBSparkJdbcSmokeTest").getOrCreate();
                try {
                    IoTDBSparkTest.copy(spark, options, "SELECT " + columns + " FROM " + source + " LIMIT 2", target);
                    List<Row> rows = spark.read().format("jdbc").options(options)
                            .option("query", "SELECT " + columns + " FROM " + target + " ORDER BY time LIMIT 3")
                            .load().collectAsList();
                    check(rows.size() == 2, "row count");
                    Row first = rows.get(0), second = rows.get(1);
                    check(first.getTimestamp(0).equals(new Timestamp(firstMillis))
                            && second.getTimestamp(0).equals(new Timestamp(firstMillis + 1000)), "timestamps");
                    check(first.getString(1).equals("sensor_a") && second.getString(1).equals("sensor_a"), "TAG");
                    check(first.getDouble(2) == 12.5 && second.getDouble(2) == -2.25, "DOUBLE");
                    check(first.getString(3).equals("O'Brien") && second.isNullAt(3), "quoted and null strings");
                    check(Arrays.equals(first.getAs(4), new byte[]{0, (byte) 255}) && second.isNullAt(4), "BLOBs");
                } finally {
                    spark.stop();
                }
            }
        }
        System.out.println("PASS Spark JDBC query/append/readback: 2 rows, 2 batches, " + database);
    }

    private static void check(boolean valid, String field) {
        if (!valid) {
            throw new IllegalStateException("Smoke test failed to verify " + field);
        }
    }
}
