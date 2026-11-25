package com.taosdata.java;

import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;

import scala.Option;

public class IoTDBDialect extends JdbcDialect {
    @Override
    public boolean canHandle(String url) {
        return IoTDBSparkDriver.usesTableDialect(url);
    }

    @Override
    public String getTableExistsQuery(String table) {
        return getSchemaQuery(table);
    }

    @Override
    public String getSchemaQuery(String table) {
        return "SELECT * FROM " + table + " LIMIT 0";
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public Option<DataType> getCatalystType(
            int sqlType, String typeName, int size, MetadataBuilder metadata) {
        if (typeName == null) {
            return Option.empty();
        }

        String normalizedType = typeName.toUpperCase(java.util.Locale.ROOT);
        if ("STRING".equals(normalizedType) || "TEXT".equals(normalizedType)) {
            return Option.apply(DataTypes.StringType);
        }
        if ("BLOB".equals(normalizedType)) {
            return Option.apply(DataTypes.BinaryType);
        }
        return Option.empty();
    }
}
