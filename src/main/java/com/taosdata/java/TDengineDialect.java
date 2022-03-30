package com.taosdata.java;

import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import scala.Option;

public class TDengineDialect extends JdbcDialect{

	@Override
	public boolean canHandle(String url) {
	  return url.startsWith("jdbc:TAOS");
	}
	@Override
	public String getTableExistsQuery(String table) {
		String destable = "select * from  " + table + " limit 0 ";
	  	return destable;
	}
	@Override
	public String getSchemaQuery(String table) {
		String destable = "select * from  " + table + " limit 1 ";
	  	return destable;
	}
	// @Override
	// public Option<DataType> getCatalystType(int sqlType,
	// 	String typeName, int size, MetadataBuilder md) {
	// 	if (typeName.toLowerCase().compareTo("timestamp") == 0) {
	// 	return Option.apply(DataTypes.StringType);
	// 	}
	// 	if (typeName.toLowerCase().compareTo(
	// 		"int") == 0) {
	// 	return Option.apply(DataTypes.StringType);
	// 	}

	// 	return Option.empty();
	// }
}
