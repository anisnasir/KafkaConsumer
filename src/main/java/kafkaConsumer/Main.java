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
		configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "9.116.35.208:9092");
		configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");
		configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		

		//Figure out where to start processing messages from
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
		kafkaConsumer.subscribe(Arrays.asList(topicName));
		//Start processing messages
		try {
			int count =0;

			while (true) {
				ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
				for(ConsumerRecord<String, String> entry: records)
					System.out.println(entry.key() + " " + entry.value());
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