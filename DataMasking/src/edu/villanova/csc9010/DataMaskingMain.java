package edu.villanova.csc9010 ;
import edu.villanova.csc9010.KafkaProducer;
import edu.villanova.csc9010.CoreMasking ;
import edu.villanova.csc9010.UDF;
import java.util.ArrayList;
import java.lang.*;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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

import edu.villanova.csc9010.MaskedAttributeFactory ;

public class DataMaskingMain implements Serializable {
	public static int rowNumber = 0 ;
    public static JavaSparkContext sc =		
                new JavaSparkContext(new SparkConf().setAppName("SparkJdbcDs").setMaster("local[*]"));
    private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(DataMaskingMain.class);
    public static void main(String[] args) {
	try{	
	/*
	MaskedAttributeFactory maskedAttributeFactory = new MaskedAttributeFactory();
	List<String> fakeNameList =	new ArrayList<String>();
	fakeNameList = maskedAttributeFactory.getFakeNames1(10) ;
	for (String fakeNames : fakeNameList) {
      System.out.println("Fake names: " + fakeNames) ;	
	}
	*/
	String tableName ;
	List<String> colList = new ArrayList<String>();
		KafkaProducer kafkaProducer = new KafkaProducer() ;
		List<Row> tbList =	new ArrayList<Row>();
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.println(" Please enter the table name followed by columns separated by space. Eg: emp name ssn " );
		String dataInput = br.readLine();
		String[] dataInputArray = dataInput.split("\\s") ;
		System.out.println(" Total args: " + dataInputArray.length ) ;
		tableName = dataInputArray[0] ;
		for (int x=1; x<dataInputArray.length; x++){
		colList.add(dataInputArray[x]) ;	
		}
		CoreMasking coreMasking = new CoreMasking();
		tbList = coreMasking.getMaskedData(tableName,colList) ;
		for (Row tbDtl : tbList) {
         KafkaProducer.PushMessageToKafka(tbDtl.toString()) ;	
			}
		}
		catch(Exception e){
		      e.printStackTrace();
		}		
    }
	
}
