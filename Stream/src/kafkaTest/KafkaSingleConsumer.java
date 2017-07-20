package kafkaTest;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaSingleConsumer {

	public static void main(String[] args) {
		
		
		 String topicName = "singleBroker1";
	     Properties props = new Properties();
	      
			        props.put("bootstrap.servers","localhost:9092");
			        props.put("group.id","test");
			        props.put("enable.auto.commit","true");
			        props.put("auto.commit.interval.ms","1000");
			        props.put("session.timeout.ms","30000");
			        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
			        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
			      
	      KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
	                                    consumer.subscribe(Arrays.asList(topicName));
	                                    
	                                    
	      while (true) 
	      {
	               ConsumerRecords<String, String> records = consumer.poll(100);
	               for (ConsumerRecord<String, String> record : records)
	                                        
	               // print the offset,key and value for the consumer records.
	               System.out.printf("offset = %d, key = %s, value = %s\n",record.offset(), record.key(), record.value());
	                                     
	      
	      
	      }
	      

	}

}
