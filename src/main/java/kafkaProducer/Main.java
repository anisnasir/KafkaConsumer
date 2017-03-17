package kafkaProducer;

import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

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
                props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
                props.put(ConsumerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
                props.put(ConsumerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String,String>(props);
     		consumer.subscribe(Arrays.asList("foo", "bar"));
     		while (true) {
         		ConsumerRecords<String, String> records = consumer.poll(100);
         		for (ConsumerRecord<String, String> record : records)
             			System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
     		}
	}

}
