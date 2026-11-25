package com.taosdata.java;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialects;

public final class IoTDBSparkTest {
    private static final String JDBC_DRIVER = "com.taosdata.java.IoTDBSparkDriver";

    private IoTDBSparkTest() {
    }

    public static void main(String[] args) {
        Map<String, String> options = jdbcOptions();
        String query = requiredEnvironmentVariable("IOTDB_READ_QUERY");
        String targetTable = requiredEnvironmentVariable("IOTDB_TARGET_TABLE");
        SparkSession spark = SparkSession.builder()
                .appName("IoTDBSparkJDBC")
                .getOrCreate();

        try {
            copy(spark, options, query, targetTable);
        } finally {
            spark.stop();
        }
    }

    static Map<String, String> jdbcOptions() {
        Map<String, String> options = new HashMap<>();
        String url = requiredEnvironmentVariable("IOTDB_JDBC_URL");
        if (!IoTDBSparkDriver.usesTableDialect(url)) {
            throw new IllegalArgumentException("IOTDB_JDBC_URL must select sql_dialect=table");
        }
        options.put("url", url);
        options.put("driver", JDBC_DRIVER);
        options.put("user", requiredEnvironmentVariable("IOTDB_USERNAME"));
        options.put("password", requiredEnvironmentVariable("IOTDB_PASSWORD"));
        options.put("isolationLevel", "NONE");
        return options;
    }

    static void copy(SparkSession spark, Map<String, String> options, String query, String target) {
        JdbcDialects.registerDialect(new IoTDBDialect());
        spark.read().format("jdbc").options(options).option("query", query).load()
                .write().format("jdbc").options(options).mode(SaveMode.Append)
                .option("dbtable", target).save();
    }

    private static String requiredEnvironmentVariable(String name) {
        String value = System.getenv(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Set the " + name + " environment variable");
        }
        return value;
    }
}
