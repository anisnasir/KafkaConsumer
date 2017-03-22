package kafkaConsumer;

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public class Main {
	public static void main(String[] args) throws Exception {
		String topicName = args[0];
		Properties configProperties = new Properties();
		configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "9.116.35.208:2181");
		configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, topicName);
		configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");
		configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "smallest");
		configProperties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY, "range");

		//Figure out where to start processing messages from
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
		kafkaConsumer.subscribe(topicName);
		//Start processing messages
		try {
			int count =0;

			while (true) {
				Map<String, ConsumerRecords<String, String>> records = kafkaConsumer.poll(100);
				System.out.println(records);
				if(count++ == 10)
					break;
			}

		}catch(Exception ex){
			System.out.println("Exception caught " + ex.getMessage());
		}finally{
			kafkaConsumer.close();
			System.out.println("After closing KafkaConsumer");
		}

	}
}