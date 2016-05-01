package edu.villanova.csc9010 ;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.Column ;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD ;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField ;

import io.codearte.jfairy.Fairy ;
import com.google.inject.Provider;
import io.codearte.jfairy.data.DataMaster;
import io.codearte.jfairy.producer.BaseProducer;
import io.codearte.jfairy.producer.DateProducer;
import io.codearte.jfairy.producer.company.Company;
import io.codearte.jfairy.producer.company.CompanyFactory;
import io.codearte.jfairy.producer.net.NetworkProducer;
import io.codearte.jfairy.producer.payment.CreditCard;
import io.codearte.jfairy.producer.person.Person;
import io.codearte.jfairy.producer.person.PersonFactory;
import io.codearte.jfairy.producer.person.PersonProperties;
import io.codearte.jfairy.producer.text.TextProducer;

import javax.inject.Inject;
import java.util.Locale;
import java.util.ArrayList;
import java.lang.*;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import scala.Tuple2;

import java.util.Iterator;
import java.util.Set;
import com.google.gson.Gson;

public class MaskedAttributeFactory {

 	private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PWD = "kanicheri";
    private static final String MYSQL_CONNECTION_URL =
            "jdbc:mysql://localhost:3306/test?user=" + MYSQL_USERNAME + "&password=" + MYSQL_PWD;
    private static final SQLContext sqlContext1 = new SQLContext(DataMaskingMain.sc);
	public static DataFrame getFakeNames1(int fakeNameCount){ // this method works fine
		List<String> fakeNameList =	new ArrayList<String>();
		Fairy fairy = Fairy.create();
		for( int i = 0; i < fakeNameCount; i++ ) {
		Person person = fairy.person();
		fakeNameList.add(person.firstName());
		}
		
		DataFrame dfFakedNameList = sqlContext1.read().json(DataMaskingMain.sc.parallelize(fakeNameList));
		List<Row> fakeNameListRow =  dfFakedNameList.collectAsList();
		return dfFakedNameList ;
	}
	public static DataFrame getFakeNamesRowID(int fakeNameCount){
		List<String> fakeNameListRowID = new ArrayList<String>();
		for( int i = 0; i < fakeNameCount; i++ ) {
			fakeNameListRowID.add(Integer.toString(i));
		}
		DataFrame dfFakedNameListRowID = sqlContext1.read().json(DataMaskingMain.sc.parallelize(fakeNameListRowID));
		return dfFakedNameListRowID ;
	}
	
	public static void getFakeNamesTest(int fakeNameCount){
		List<Tuple2<String,String>> fakeNameList = new ArrayList<Tuple2<String,String>>();
		Fairy fairy = Fairy.create();
		for( int i = 0; i < fakeNameCount; i++ ) {
		Person person = fairy.person();
		fakeNameList.add(new Tuple2<String,String>(Integer.toString(i), person.firstName()));
		}
		//List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
		//JavaRDD<Integer> distData = sc.parallelize(data);
		/*JavaRDD<Integer> jRDD = DataMaskingMain.sc.parallelize(fakeNameList);
		List<StructField> fields = new ArrayList<StructField>();
			fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
			StructType schema = DataTypes.createStructType(fields);
		DataFrame dfFakedNameList = sqlContext1.createDataFrame(jRDD, schema);
		*/
		//String json = new Gson().toJson(fakeNameList);
		//DataFrame df = sqlContext1.read().json(json);
		//DataFrame df = sqlContext1.read().json(new Gson().toJson(fakeNameList));
		
		//JavaPairRDD<String,String> JPR = sqlContext1.parallelizePairs(fakeNameList);
		// JavaPairRDD<String,String> JPR = DataMaskingMain.sc.parallelizePairs(fakeNameList);
		 //JavaRDD.fromRDD(JavaPairRDD.toRDD(rdd), rdd.classTag());
		//DataFrame dfFakedNameList = sqlContext1.createDataFrame(JPR, TextId.class);
		
		//DataFrame dfFakedNameList = JPR.toDF(); //sqlContext1.read().json(DataMaskingMain.sc.parallelize(fakeNameList));
		//DataFrame dfFakedNameList = sqlContext1.read().json(DataMaskingMain.sc.parallelize(fakeNameList));
		//System.out.println("count is ===================> " + dfFakedNameList.count());
		//List<Row> fakeNameListRow =  dfFakedNameList.collectAsList();
		//return dfFakedNameList ;
		
	}
    
			public static class RDDobj implements Serializable {
			private String id;
			private String name;
			public String getName() {
				return name;
			}
			public String getId() {
				return id;
			}
			public void setName(String name) {
				this.name = name;
			  }

			 public void setId(String id) {
				this.id = id;
			  }
			}
 }