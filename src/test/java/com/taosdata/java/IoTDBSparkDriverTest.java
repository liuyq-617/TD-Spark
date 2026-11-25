package com.taosdata.java;

import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.*;

public class IoTDBSparkDriverTest {
    @Test
    public void selectsOnlyAnExplicitTableEndpoint() throws Exception {
        IoTDBSparkDriver driver = new IoTDBSparkDriver();
        assertTrue(driver.acceptsURL("jdbc:iotdb://localhost:6667?sql_dialect=table"));
        assertTrue(driver.acceptsURL("jdbc:iotdb://localhost:6667/db?zone_id=UTC&sql_dialect=table"));
        for (String suffix : Arrays.asList("", "?not_sql_dialect=table", "?sql_dialect=tree",
                "?sql_dialect=table&sql_dialect=table", "?sql_dialect=table#foo")) {
            assertNull(driver.connect("jdbc:iotdb://localhost:6667" + suffix, null));
        }
        assertFalse(driver.acceptsURL("jdbc:iotdb://user@localhost:6667?sql_dialect=table"));
    }

    @Test
    public void bindsBinaryAndNegativeMicrosecondsAndClearsEachBatch() throws Exception {
        List<String> calls = new ArrayList<>();
        SQLException rejected = new SQLException("test batch rejected");
        PreparedStatement jdbc = (PreparedStatement) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{PreparedStatement.class}, (proxy, method, args) -> {
                    calls.add(method.getName());
                    switch (method.getName()) {
                        case "setBinaryStream":
                            InputStream input = (InputStream) args[1];
                            assertEquals(2, args[2]);
                            assertEquals(0, input.read());
                            assertEquals(255, input.read());
                            return null;
                        case "setLong":
                            assertEquals(-2L, args[1]);
                            return null;
                        case "executeBatch":
                            if (calls.contains("clearBatch")) throw rejected;
                            return new int[]{1};
                        default:
                            return null;
                    }
                });
        PreparedStatement adapted = IoTDBSparkDriver.sparkStatement(jdbc, 1_000_000);
        adapted.setBytes(1, new byte[]{0, (byte) 255});
        adapted.setTimestamp(2, Timestamp.from(Instant.ofEpochSecond(-1, 999_998_999)));
        assertArrayEquals(new int[]{1}, adapted.executeBatch());
        assertSame(rejected, assertThrows(SQLException.class, adapted::executeBatch));
        assertEquals(Arrays.asList("setBinaryStream", "setLong", "executeBatch", "clearBatch",
                "executeBatch", "clearBatch"), calls);
    }
}
