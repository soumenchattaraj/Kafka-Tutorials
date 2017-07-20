package kafkaTest;

import java.util.Properties;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import kafka.admin.AdminUtils;
import kafka.utils.*;
import kafka.utils.ZkUtils;

public class KafkaTopicCreation {
	
	public void KakfaNewTopic(String createTopic)
	{
		ZkClient zkClient = null;
        ZkUtils zkUtils = null;
        
        //If multiple zookeeper then -> String zookeeperHosts = "192.168.20.1:2181,192.168.20.2:2181";
        String zookeeperHosts = "192.168.20.1:2181"; 
        
        int sessionTimeOutInMs = 15 * 1000; // 15 secs
        int connectionTimeOutInMs = 10 * 1000; // 10 secs
        
        zkClient = new ZkClient(zookeeperHosts, sessionTimeOutInMs, connectionTimeOutInMs, ZKStringSerializer$.MODULE$);
        
        //## below line will be required if kafka version is higher than 0.9
        zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperHosts), false);
        
        
        String topicName = createTopic;
        int noOfPartitions = 1; // ## for multiple broker configuration partitions will be 2 & single broker : 1
        int noOfReplication = 3;
        Properties topicConfiguration = new Properties();
        
      // AdminUtils.createTopic(zkUtils, topicName, noOfPartitions, noOfReplication, topicConfiguration);
        AdminUtils.createTopic(zkUtils, topicName, noOfPartitions, noOfReplication, topicConfiguration, null);
        zkClient.close();
	}

	
}
