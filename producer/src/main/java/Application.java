import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Application {

    private static final String TOPIC = "events";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092, localhost:9093, localhost:9094";

    public static void main(String[] args) {
        Producer<Long, String> kafkaProducer = createKafkaProducer(BOOTSTRAP_SERVERS);
        try {
            produceMessages(10, kafkaProducer);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        finally {
            kafkaProducer.close();
            kafkaProducer.flush();
        }
    }
    private static void produceMessages(int numOfMessages, Producer<Long, String> producer) throws ExecutionException, InterruptedException {

        int partition = 0;
        for (int i = 0;i<numOfMessages; i++) {
            long key = i;
            String value = String.format("event %d" , i);
            long timestamp = System.currentTimeMillis();

            ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, value);
            RecordMetadata metadata = producer.send(record).get();

            System.out.println(String.format("Record with ( key: %s, value: %s) was sent partition: %d  and offset: %d",
                    record.key(), record.value(), metadata.partition(), metadata.offset()));
        }
    }
    private static Producer<Long, String> createKafkaProducer(String bootstrapServers) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "event-producer");

        return new KafkaProducer<Long, String>(properties);
    }
}
