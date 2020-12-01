import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Application {

    private static final String TOPIC = "events";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092, localhost:9093, localhost:9094";
    public static void main(String[] args) {
        String consumerGroup = "defaultConsumerGroup";
        if(args.length == 1)
            consumerGroup = args[0];

        System.out.println("Consumer is part of group : " + consumerGroup);
        Consumer<Long, String> consumer = createConsumer(BOOTSTRAP_SERVERS, consumerGroup);
        consumeMessages(TOPIC, consumer);

    }
    private static void consumeMessages(String topic, Consumer<Long, String> consumer) {
        consumer.subscribe(Collections.singletonList(topic)); // As there can be more than one topics the group subscribes to
        while (true) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if(consumerRecords.isEmpty()) {
                //System.out.println("Oi, what do we have here ??");
            }

            for (ConsumerRecord<Long, String> record : consumerRecords) {
                System.out.println(String.format("Record with ( key: %s, value: %s) was sent partition: %d  and offset: %d",
                        record.key(), record.value(), record.partition(), record.offset()));
            }

            // Process the message here before committing
            consumer.commitAsync();
        }
    }
    private static Consumer<Long, String> createConsumer(String bootstrapServers, String consumerGroup) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Important in order to manually commit after consuming any message

        return new KafkaConsumer<Long, String>(properties);
    }
}