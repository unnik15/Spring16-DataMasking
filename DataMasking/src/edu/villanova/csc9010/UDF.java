package edu.villanova.csc9010 ;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.types.DataTypes ;

public class UDF implements UDF1<String, String> {
	private transient JavaSparkContext sc;
    private transient SQLContext sqlContext;
    @Override
    public String call(String colName) throws Exception {
		if (colName.trim().length() > 0 ){
			return colName.substring(0,1) + "##########" ;
		}
		else
		{
			return null ;
		}
      }
}
