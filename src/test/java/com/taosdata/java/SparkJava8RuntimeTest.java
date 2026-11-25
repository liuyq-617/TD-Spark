package com.taosdata.java;

import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SparkJava8RuntimeTest {
    @Test
    public void startsSparkAndExecutesAQuery() {
        SparkSession spark = SparkSession.builder()
                .appName("SparkJava8RuntimeTest")
                .master("local[1]")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();
        try {
            assertEquals(1L, spark.range(1).count());
        } finally {
            spark.stop();
        }
    }
}
