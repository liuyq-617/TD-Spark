package com.taosdata.java;

import java.io.ByteArrayInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.Properties;

import org.apache.iotdb.jdbc.IoTDBConnection;
import org.apache.iotdb.jdbc.IoTDBDriver;
import org.apache.thrift.transport.TTransportException;

public final class IoTDBSparkDriver extends IoTDBDriver {
    @Override
    public boolean acceptsURL(String url) {
        return usesTableDialect(url);
    }

    static boolean usesTableDialect(String url) {
        if (url == null || !url.startsWith("jdbc:iotdb://")) {
            return false;
        }
        try {
            URI endpoint = new URI(url.substring("jdbc:".length()));
            if (endpoint.getHost() == null || endpoint.getPort() < 1 || endpoint.getPort() > 65535
                    || endpoint.getUserInfo() != null || endpoint.getFragment() != null
                    || endpoint.getRawPath().indexOf('/', 1) >= 0 || endpoint.getRawQuery() == null) {
                return false;
            }
            boolean table = false;
            for (String parameter : endpoint.getRawQuery().split("&", -1)) {
                String[] pair = parameter.split("=", -1);
                if (pair.length != 2 || pair[0].isEmpty() || pair[1].isEmpty()) {
                    return false;
                }
                if (pair[0].equals("sql_dialect")) {
                    if (table || !pair[1].equals("table")) {
                        return false;
                    }
                    table = true;
                }
            }
            return table;
        } catch (URISyntaxException invalidUrl) {
            return false;
        }
    }

    @Override
    public Connection connect(String url, Properties credentials) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        int query = url.indexOf('?');
        String jdbcUrl = url.charAt(query - 1) == '/'
                ? url.substring(0, query - 1) + url.substring(query) : url;
        try {
            return new SparkConnection(jdbcUrl, credentials == null ? new Properties() : (Properties) credentials.clone());
        } catch (TTransportException failure) {
            throw new SQLException("Could not open the IoTDB RPC connection", failure);
        }
    }

    private static final class SparkConnection extends IoTDBConnection {
        private SparkConnection(String url, Properties credentials) throws SQLException, TTransportException {
            super(url, credentials);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int type, int concurrency) throws SQLException {
            if (type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY) {
                return super.prepareStatement(sql);
            }
            return super.prepareStatement(sql, type, concurrency);
        }

        @Override
        public PreparedStatement prepareStatement(String sql) throws SQLException {
            return sparkStatement(super.prepareStatement(sql), getTimeFactor());
        }
    }

    static PreparedStatement sparkStatement(PreparedStatement statement, int ticksPerSecond) {
        return (PreparedStatement) Proxy.newProxyInstance(IoTDBSparkDriver.class.getClassLoader(),
                new Class<?>[]{PreparedStatement.class}, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "executeBatch":
                            // IoTDB JDBC retains its SQL list across executeBatch() calls.
                            try {
                                int[] counts = statement.executeBatch();
                                statement.clearBatch();
                                return counts;
                            } catch (SQLException failure) {
                                try {
                                    statement.clearBatch();
                                } catch (SQLException clearFailure) {
                                    failure.addSuppressed(clearFailure);
                                }
                                throw failure;
                            }
                        case "setBytes":
                            byte[] bytes = (byte[]) args[1];
                            if (bytes == null) {
                                statement.setNull((int) args[0], Types.BINARY);
                            } else {
                                statement.setBinaryStream((int) args[0], new ByteArrayInputStream(bytes), bytes.length);
                            }
                            return null;
                        case "setTimestamp":
                            if (args.length == 2) {
                                if (args[1] == null) {
                                    statement.setNull((int) args[0], Types.TIMESTAMP);
                                    return null;
                                }
                                Instant instant = ((Timestamp) args[1]).toInstant();
                                long seconds = Math.multiplyExact(instant.getEpochSecond(), ticksPerSecond);
                                long fraction = instant.getNano() / (1_000_000_000 / ticksPerSecond);
                                statement.setLong((int) args[0], Math.addExact(seconds, fraction));
                                return null;
                            }
                            break;
                        default:
                            break;
                    }
                    return invoke(statement, method, args);
                });
    }

    private static Object invoke(PreparedStatement statement, Method method, Object[] arguments) throws Throwable {
        try {
            return method.invoke(statement, arguments);
        } catch (InvocationTargetException failure) {
            throw failure.getCause();
        }
    }
}
