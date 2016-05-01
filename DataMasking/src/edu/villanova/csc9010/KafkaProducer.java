package edu.villanova.csc9010 ;
import java.sql.*;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import scala.collection.Seq;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class KafkaProducer {
   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
   static final String DB_URL = "jdbc:mysql://localhost/test";
   //  Database credentials
   static final String USER = "root";
   static final String PASS = "kanicheri";
   final static String TOPIC = "datamasking";
   public static void PushMessageToKafka(String msg)
	{
		try{
      	    Properties properties = new Properties();
			properties.put("metadata.broker.list","localhost:9092");
			properties.put("serializer.class","kafka.serializer.StringEncoder");
			ProducerConfig producerConfig = new ProducerConfig(properties);
			kafka.javaapi.producer.Producer<String,String> producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);
			SimpleDateFormat sdf = new SimpleDateFormat();
			KeyedMessage<String, String> message =new KeyedMessage<String, String>(TOPIC, msg + sdf.format(new Date()));
			producer.send(message);
			producer.close();
		}
		catch(Exception e){
		      e.printStackTrace();
		}
		finally{
			//finally block used to close resources
			msg = null;
		}//end finally try
   }
	
}
