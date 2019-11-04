import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.common.collect.Lists;

//Hosebird client(hbc) - Java HTTP library for consuming Twitter Streaming API.
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;


public class TwitterStreamProducer {
    private final static String CLIENT_ID = "Client_1";

    
    public static void pushTwitterMessages(Producer<Long,String> producer,
    		    						     final String topic,
    		    						     final String trackTerms,
    		    						     final int maxMsgNum)
    	        throws InterruptedException {    	
    	ProducerRecord<Long,String> message = null;
    	BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
    	
    	// setup term tracking on recent status messages
    	StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
    	endpoint.trackTerms(Lists.newArrayList(trackTerms.split(",")));
    	//endpoint.trackTerms(Lists.newArrayList("#AI", "#BigData"));
    	  	
    	Authentication auth = null;
    	try {
    		auth = authenticateUser();
    	} catch(IOException ex) {
    		 ex.printStackTrace();
    	}
    	
    	/* connect to Twitter, fetch messages from queue and
 	   	   send them through to the Kafka Producer instance */
    	Client client = new ClientBuilder()
    			.hosts(Constants.STREAM_HOST)
    			.endpoint(endpoint)
    			.authentication(auth)
    			.processor(new StringDelimitedProcessor(queue))
    			.build();
    	
    	client.connect();
    	System.out.println("Connected to Twitter(dot)com. Streaming tweets...\n");
    	
    	for (int readMsgCount=1; readMsgCount <= maxMsgNum; readMsgCount++) {
    		try {
    			String msg = queue.take();
    			System.out.print(readMsgCount + " " + msg);
    			message = new ProducerRecord<Long,String>(topic, msg);
    		} catch(InterruptedException e) {
    			e.printStackTrace();
    		}
    		producer.send(message);
    	}
    	System.out.println("\nFetched a total of " + maxMsgNum + " tweets from Twitter(dot)com!\nExiting...\n");
    	producer.close();
    	client.stop();
    }
    
    
    
    private static Producer<Long,String> initProducer(final String bootstrap_server) {
    	Properties props = new Properties(); 
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<Long, String>(props);
    }
    
    
    
    private static Authentication authenticateUser() throws IOException {    	
    	String fileName = "credentials.properties";
    	InputStream  ins = null;
    	Properties prop = null;
    	
    	try {
    		ins = TwitterStreamProducer.class.getResourceAsStream(fileName);
    		prop = new Properties();
    		if (ins != null)
    			prop.load(ins);
         } catch(IOException ex) {
            ex.printStackTrace();
         } finally {
            ins.close();
         }
    	
    	String consumerKey = prop.getProperty("consumerkey");
    	String consumerSecret = prop.getProperty("consumersecret");
    	String token =  prop.getProperty("token");
    	String secret = prop.getProperty("secret");
    	
    	return new OAuth1(consumerKey, consumerSecret, token, secret);
    }
        
//==================================== main() =======================================    
    
    public static void main(String[] args) {    	    	
    	if (args.length == 4) {
    	    try {
    	    	String bootstrap_server = args[0];
    	    	String toTopic = args[1];
    	        String trackTerms = args[2];
    	        int maxMsgNum = Integer.parseInt(args[3]);
    	        
    	        Producer<Long,String> producer = initProducer(bootstrap_server);
    	        pushTwitterMessages(producer, toTopic, trackTerms, maxMsgNum);
    	    } catch (NumberFormatException e) {
    	        System.err.println("Argument " + args[3] + " must be an integer.");
    	        System.exit(1);
    	    } catch(InterruptedException e) {
            	System.out.println(e);
    	    }
    	} else {
    		System.out.println("Correct usage: '$> java  TwitterStreamProducer  <bs_server_ip:port#>  <topic_name>  <hashtag1,hastag2,...>  <max_msgs_num>'");
    	}
    }
}