package kafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Main {
	public static void main(String[] args) throws Exception{

		// Check arguments length value
		if(args.length == 0){
			System.out.println("test");
			return;
		}

		//Assign topicName to string variable
		String topicName = args[0].toString();

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");  
		props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		props.put("partition.assignment.strategy", "range");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String,String>(props);
		consumer.subscribe(topicName);

		try {
			while (true) {
				Map<String, ConsumerRecords<String, String>> records = consumer.poll(1000);
				System.out.println(records);
			}
		} finally {
			consumer.close();
		}
	}

}
