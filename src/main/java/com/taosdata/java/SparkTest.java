package com.taosdata.java;                

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
public class  SparkTest{
    public static void main(String[] args) {
		// 数据库配置
		String url = "jdbc:TAOS://u05:6030/tt?user=root&password=taosdata";
		String driver = "com.taosdata.jdbc.rs.TSDBDriver";
        String dbtable = "t1";
		
		SparkSession sparkSession = SparkSession.builder()
				.appName("DataSourceJDBC") // 设置应用名称
				.master("local") // 本地单线程运行
				.getOrCreate();

		JdbcDialect  tdengineDialect = new TDengineDialect();
		JdbcDialects.registerDialect(tdengineDialect);
		// 创建DataFrame
		Dataset<Row> df = sparkSession
				.read() // 返回一个DataFrameReader，可用于将非流数据作为DataFrame读取
				.format("jdbc") // JDBC数据源
				.option("url", url)
				.option("driver", driver)
				.option("query", "select * from tt.t1 limit 100") // 二选一，sql语句或者表
                // .option("dbtable", "tt.t1")
                // .option("fetchsize", 1000)
                // .option("pushDownPredicate", false)
				.load();
                // .where("");

		// 将DataFrame的内容显示
		df.show();

        
		// *** 写入数据 ***
		df.write() // 返回一个DataFrameWriter，可用于将DataFrame写入外部存储系统
				.format("jdbc") // JDBC数据源
				.mode(SaveMode.Append) // 如果第一次生成了，后续会追加
				.option("url", url)
				.option("driver", driver)
				.option("dbtable", "tt.t2") // 表名
				.save();


		sparkSession.stop();

	}
}