package kafkaTest;

import java.util.Properties;
import java.util.Scanner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;



public class KafkaSingleProducer {

	public static void main(String[] args) {
		
		String topicName ="singleBroker1";
		Properties prop  = new Properties();
				   
		          //Assign localhost id
				   prop.put("bootstrap.servers","localhost:9092");
				 //Set acknowledgements for producer requests.      
				   prop.put("acks","all");
				 //If the request fails, the producer can automatically retry,
				   prop.put("retries", 0);
				 //Specify buffer size in config
				   prop.put("batch.size", 16384);
				 //Reduce the no of requests less than 0   
				   prop.put("linger.ms", 1);
				 //The buffer.memory controls the total amount of memory available to the producer for buffering.   
				   prop.put("buffer.memory", 33554432);
				   prop.put("key.serializer","org.apache.kafka.common.serializa-tion.StringSerializer");
				   prop.put("value.serializer","org.apache.kafka.common.serializa-tion.StringSerializer");
				   
				   
				   
				   Scanner sc = new  Scanner(System.in);
				   String  strMsg = null;
				   do
				   {	
				  // String  
					strMsg  = sc.nextLine();
				   
				   //StringBuffer strBuf = new  StringBuffer();
				  //  StringBuffer strMsg  = strBuf.append(sc.nextLine());
				   System.out.println();
				   
				   Producer<String, String> producer = new KafkaProducer<String, String>(prop);
				                            producer.send(new ProducerRecord<String, String>(topicName,strMsg,strMsg));
				                            
				                            
				                            producer.close();
				   System.out.println("*** Message Sent ***");
		
				   }while(strMsg.equalsIgnoreCase("null"));

	}

}
