package edu.villanova.csc9010 ;
import edu.villanova.csc9010.UDF;
import java.util.ArrayList;
import java.lang.*;
import java.io.*;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.Column ;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.functions ;
//import org.apache.spark.sql.functions.monotonicallyIncreasingId ;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;

import edu.villanova.csc9010.MaskedAttributeFactory ;

public class CoreMasking {

    private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(DataMaskingMain.class);
	private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PWD = "kanicheri";
    private static final String MYSQL_CONNECTION_URL =
            "jdbc:mysql://localhost:3306/test?user=" + MYSQL_USERNAME + "&password=" + MYSQL_PWD;

	private static int rn = 0;
    private static final SQLContext sqlContext = new SQLContext(DataMaskingMain.sc);
	private String tableName ;
	private List<String> colList = new ArrayList<String>();
	public static List<Row> getMaskedData(String tableName,List<String> colList){
		List<Row> tbColsList =	new ArrayList<Row>();
		tbColsList = getTBColumns(tableName) ;
 		Map<String, String> options = new HashMap<>();
        options.put("driver", MYSQL_DRIVER);
        options.put("url", MYSQL_CONNECTION_URL);
		options.put("dbtable",tableName);
        DataFrame df_data = sqlContext.load("jdbc", options);
		sqlContext.registerDataFrameAsTable(df_data, "df_data_tb");
		UDF udf = new UDF();
		final String udfName = "maskColumn";
		final String dfTable = "df_data_tb";
		sqlContext.udf().register(udfName, udf, DataTypes.StringType);
   	    String colSelected = "" ;
		int col_masked_flag = 2;
		for (int i = 0; i < tbColsList.size(); i++) {
		for (int j = 0; j < colList.size(); j++) {
			if (tbColsList.get(i).get(0).toString().trim().toUpperCase().equals(colList.get(j).trim().toUpperCase())){
				colSelected = colSelected + udfName + "("  + dfTable + "." + colList.get(j) + ")," ;
				col_masked_flag = 1;
				break;
			}
			else
			{
				col_masked_flag = 2;
			}
		}
		if (col_masked_flag == 2)
			{
			colSelected = colSelected +  dfTable + "." + tbColsList.get(i).get(0) + "," ;
			}
		}
		if (colSelected != null && colSelected.length() > 0 && colSelected.charAt(colSelected.length()-1)==',') {
			colSelected = colSelected.substring(0, colSelected.length()-1);
			}
		String stmt = "SELECT " + colSelected + " FROM " + dfTable ;
		DataFrame df_tb_masked = sqlContext.sql(stmt); 
		List<Row> tbRow =  df_tb_masked.collectAsList();
		return tbRow ;
	}
	
	public static List<Row> getTBColumns(String tableName){
		Map<String, String> options = new HashMap<>();
        options.put("driver", MYSQL_DRIVER);
        options.put("url", MYSQL_CONNECTION_URL);
		options.put("dbtable","INFORMATION_SCHEMA.COLUMNS");
        DataFrame df_data = sqlContext.load("jdbc", options);
		sqlContext.registerDataFrameAsTable(df_data, "df_data_tb1");
		String stmt = "SELECT COLUMN_NAME FROM df_data_tb1 WHERE TABLE_SCHEMA = 'test' and TABLE_NAME = '" + tableName + "'";
		DataFrame df_tb_cols = sqlContext.sql(stmt); 
		List<Row> tbCols =  df_tb_cols.collectAsList();
		return tbCols ;
		}
	public static void getMaskedDataWithFakeData(String tableName,List<String> colList){
			//public static List<Row> getMaskedDataWithFakeData(String tableName,List<String> colList){
			MaskedAttributeFactory maskedAttributeFactory = new MaskedAttributeFactory();
			/*List<Row> fakeNameList =	new ArrayList<Row>();
			fakeNameList = maskedAttributeFactory.getFakeNames(5) ;
			for (Row fakeNames : fakeNameList) {
				System.out.println("Fake names: " + fakeNames.toString()) ;	
			}
			*/
			/*DataFrame dfFakedNameList = maskedAttributeFactory.getFakeNames(5) ;
			sqlContext.registerDataFrameAsTable(dfFakedNameList, "dfFakedNameListTT");
			DataFrame dfFakedNameListTest = maskedAttributeFactory.getFakeNames(5) ;
			*/
			//DataFrame dfFakedNameListRowID = maskedAttributeFactory.getFakeNamesRowID(5) ;
			//sqlContext.registerDataFrameAsTable(dfFakedNameListRowID, "dfFakedNameListRowIDTT");
			
				/*sqlContext.udf().register("AutoNum", new UDF1<String, String>() {
				@Override
				public String call(String rownum) {
			    //int r = rownum + 1;
				String strRownum =  rownum + "0" ;//r.toString();
				return strRownum ;
				}
				}, DataTypes.StringType);
				*/
				//DataMaskingMain.rowNumber = 0;
				//rn = 0 ;
			/*
				sqlContext.udf().register("AutoNum", new UDF1<Integer, Integer>() {
				@Override
				public Integer call(Integer rownum) {
				int rn1 = rn ;
				rn = rn1 + 1 ;
				return rn1;
				}
				}, DataTypes.IntegerType);
			rn = 0 ;
			String sqlString = "SELECT * from dfFakedNameListTT tt"; 
			DataFrame dfFakedNameList1 = sqlContext.sql(sqlString) ; //"SELECT AutoNum(" + DataMaskingMain.rowNumber + ") as id, tt.* from dfFakedNameListTT tt"); 
			List<Row> tbList =  dfFakedNameList1.collectAsList();
			for (Row tbDtl : tbList) {
				System.out.println("===========================> New output here:" + tbDtl.toString()) ;	
			}*/
		/*List<Row> tbColsList =	new ArrayList<Row>();
		tbColsList = getTBColumns(tableName) ;
 		Map<String, String> options = new HashMap<>();
        options.put("driver", MYSQL_DRIVER);
        options.put("url", MYSQL_CONNECTION_URL);
		options.put("dbtable",tableName);
        DataFrame df_data = sqlContext.load("jdbc", options);
		sqlContext.registerDataFrameAsTable(df_data, "df_data_tb");
		//new code for joining
		rn = 0 ;
		String sqlString1 = "SELECT AutoNum(" + rn + ") as id, tt.* from df_data_tb tt";
		DataFrame dfEmp = sqlContext.sql(sqlString1) ; 
		System.out.println("RN value 5     ===================> " + rn) ;
		List<Row> tbList1 =  dfEmp.collectAsList();
		System.out.println("Lets check the EMP table output here ======================================== ");
			for (Row tbDtl1 : tbList1) {
				System.out.println("output here:" + tbDtl1.toString()) ;	
			}
		*/
		//new code for joining ends
		
	}
	
}