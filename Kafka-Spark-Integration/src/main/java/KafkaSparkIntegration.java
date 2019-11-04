import java.io.FileWriter;
import java.io.PrintWriter;

import java.io.IOException;

import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.Collections;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;


public class KafkaSparkIntegration {
//====================== Class  Variables =============================================
//=====================================================================================
	//Spark app configuration.
	// "*" means use as many threads as there're processors available to JVM
	private static final String MASTER = "local[*]"; 
	private static final String APP_NAME ="TwitterKafkaStream";
	
	private static final String jsonTweetsFile = "JsonTweets.txt";
	
//====================== Class Methods =============================================
//==================================================================================
    /**
	 * Creates a connection link between Kafka messaging service and Spark. 
	 * It continuously accepts incoming data stream from a remote Kafka Broker
	 * server, processes the data and then saves it to a database.
	 * It iterates over each record of incoming RDD, where every record is a
	 * tweet from Twitter.com in form of a JSON formatted string. Each record then
	 * is saved to a text file. After RDD is processed the data from the text file
	 * is loaded back into a DataFrame, transformed and saved to a database.
	 * @return      void
	 */
	//TODO - Pass 'host', 'port' 'topic', etc. as command line args
	public static void runKafkaSparkIntegration(final String brokerServer, final String fromTopic) {
		//Define Spark application configuration
		final SparkConf conf = new SparkConf()
							        .setMaster(MASTER)
				                    .setAppName(APP_NAME);
		
		//Start SparkContext & print the configuration
        final JavaSparkContext sc = new JavaSparkContext(conf);
        //System.out.println(sc.getConf().toDebugString());
        
        //Create SparkSesison object
		final SparkSession ss = SparkSession.builder()
					             .master(MASTER)
					             .appName("JSONreader")
					             .getOrCreate();
        
		//Create StreamingContext obj - main entry point for Spark functionality
		final JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(5));
		//jsc.checkpoint("checkpoint");
		
		//Kafka Broker configuration
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", brokerServer);
		kafkaParams.put("auto.offset.reset", "smallest");
		Set<String> topics = Collections.singleton(fromTopic);
		
		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils
				              				  .createDirectStream(jsc, String.class, String.class,
				              						  	          StringDecoder.class, StringDecoder.class,
				              						  	          kafkaParams, topics
				              						             );
		
		//Display received messages details
		directKafkaStream.foreachRDD(rdd -> {
			System.out.println("\n==================== RDD Start ======================\n");
			long recordsCount = rdd.count();
			System.out.println("New RDD with " + recordsCount + " records.");
			
			rdd.foreach(record -> {
				System.out.println("Received record: " + record._2);
				String jsonStr = record._2;
				
				if ( jsonStr != null && !jsonStr.trim().isEmpty() )
					saveRddRecordToFile(jsonStr, jsonTweetsFile);
			});
			System.out.println("\n==================== RDD End ======================\n");

			if ( recordsCount > 0 ) {
				Dataset<Row> df = loadRddRecordsFileIntoDataFrame(ss);
				saveEachRddToStorage(ss, df);
			}
			deleteRddRecordsFile();
		});
		
		jsc.start();
		try { 
			jsc.awaitTermination();
		} catch(InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	
	
	private static void saveRddRecordToFile(String record, String fileName) {
		try {
			FileWriter fw = new FileWriter(fileName, true);
			PrintWriter writer = new PrintWriter(fw);
			writer.println(record);
			writer.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	
	
	private static Dataset<Row> loadRddRecordsFileIntoDataFrame(SparkSession ss) {
		Path tweetsFilePath = Paths.get(jsonTweetsFile);
		
		if ( Files.exists(tweetsFilePath) ) {
			Dataset<Row> tweetsDF = ss.read().format("json").load(jsonTweetsFile);
			tweetsDF.show();
			
			//Select specified columns to be saved to db table
			Dataset<Row> transformedDF = tweetsDF
						               	  .select("id_str","timestamp_ms","text")
						               	  .withColumnRenamed("id_str", "id")
						               	  .withColumnRenamed("text", "tweet_text");
			transformedDF.show();
			return transformedDF;
		}
		return null;
	}
	
	

	private static boolean saveEachRddToStorage(SparkSession ss, Dataset<Row> df) {
		if (df == null) return false;
		
		//PostrgreSQL db connection configuration
		String db_connection = "jdbc:postgresql://localhost:5432/tweets-db";
		String db_table = "tweets";
		
		Properties db_props = new Properties();
		db_props.put("user","super");
		db_props.put("password","super");
		db_props.put("driver", "org.postgresql.Driver");

		df.write().mode(SaveMode.Append).jdbc(db_connection, db_table, db_props);
		return true;
	}
	
	
	
	private static void deleteRddRecordsFile() {
		Path tweetsFilePath = Paths.get(jsonTweetsFile);
		
		try { //Delete "jsonTweetsFile" after saving its content to db
            Files.deleteIfExists(tweetsFilePath); 
        } catch(NoSuchFileException e) { 
            System.out.println("No such file/directory exists!");
            e.printStackTrace();
        } catch(DirectoryNotEmptyException e) { 
            System.out.println("Directory is not empty.");
            e.printStackTrace();
        } catch(IOException e) { 
            System.out.println("Invalid permissions."); 
            e.printStackTrace();
        }
	}
	
//====================== Main Function =============================================
//==================================================================================	
	
	public static void main(String[] args) {
		if (args.length == 2) {
	    	//Kafka broker servers configuration.
	    	final String brokerServer = args[0];
	    	final String fromTopic = args[1];
	        
	        runKafkaSparkIntegration(brokerServer, fromTopic);
    	} else {
    		System.out.println("Correct usage: '$> java  KafkaSparkIntegration  <broker_server_ip:port#>  <topic_name>'");
    	}
		
	}
	
}
