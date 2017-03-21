package kafkaConsumer;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public class Main {
	public static void main(String[] args) throws Exception {
		String topicName = args[0];
		Properties props = new Properties();
		props.put("bootstrap.servers", "9.116.35.208:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("partition.assignment.strategy", "range");

		//Figure out where to start processing messages from
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
		kafkaConsumer.subscribe(topicName);
		//kafkaConsumer.subscribe(Arrays.asList(topicName));
		//Start processing messages
		int count = 0;
		try {
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