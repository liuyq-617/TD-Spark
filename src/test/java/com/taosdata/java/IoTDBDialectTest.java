package com.taosdata.java;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.iotdb.jdbc.IoTDBDriver;
import org.junit.Test;

import scala.Option;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IoTDBDialectTest {
    private final IoTDBDialect dialect = new IoTDBDialect();

    @Test
    public void handlesIoTDBUrlsWithoutClaimingOtherDrivers() {
        assertTrue(dialect.canHandle("jdbc:iotdb://localhost:6667/?sql_dialect=table"));
        assertFalse(dialect.canHandle("JDBC:IOTDB://localhost:6667/"));
        assertFalse(dialect.canHandle("jdbc:TAOS://localhost:6030/"));
        assertFalse(dialect.canHandle(null));
    }

    @Test
    public void officialDriverAcceptsTheDocumentedTableModeUrl() throws Exception {
        assertTrue(new IoTDBDriver().acceptsURL(
                "jdbc:iotdb://localhost:6667/?sql_dialect=table"));
    }

    @Test
    public void usesBoundedTableQueriesForSparkMetadata() {
        assertEquals("SELECT * FROM telemetry LIMIT 0", dialect.getTableExistsQuery("telemetry"));
        assertEquals("SELECT * FROM telemetry LIMIT 0", dialect.getSchemaQuery("telemetry"));
        assertEquals("\"a\"\"b\"", dialect.quoteIdentifier("a\"b"));
    }

    @Test
    public void mapsIoTDBStringAndBlobTypesToSparkTypes() {
        Option<?> stringType = dialect.getCatalystType(
                java.sql.Types.VARCHAR, "STRING", 0, new MetadataBuilder());
        Option<?> textType = dialect.getCatalystType(
                java.sql.Types.VARCHAR, "text", 0, new MetadataBuilder());
        Option<?> blobType = dialect.getCatalystType(
                java.sql.Types.BLOB, "BLOB", 0, new MetadataBuilder());

        assertEquals(DataTypes.StringType, stringType.get());
        assertEquals(DataTypes.StringType, textType.get());
        assertEquals(DataTypes.BinaryType, blobType.get());
        assertFalse(dialect.getCatalystType(
                java.sql.Types.INTEGER, "INT32", 0, new MetadataBuilder()).isDefined());
    }
}
