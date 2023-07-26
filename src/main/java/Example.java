import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Example {
    static KafkaProducer<Integer, GenericRecord> producer;
    public Example() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", "localhost:8081");

        producer = new KafkaProducer<>(props);

        String schemaString = "{\"namespace\": \"customer.avro\"," +
                "\"type\": \"record\", " +
                "\"name\": \"Customer\"," +
                "\"fields\": [" +
                "{\"name\": \"id\", \"type\": \"int\"}," +
                "{\"name\": \"name\", \"type\": \"string\"}," +
                "{\"name\": \"fax\", \"type\": " + "[\"null\",\"string\"], " +
                "\"default\":\"null\" }" +
                "]}";

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaString);

        for(int i=0; i < 1000000; i++) {
            Customer customer = CustomerGenerator.getNext();
            GenericRecord genericRecord = new GenericData.Record(schema);
            genericRecord.put("id", customer.getCustomerID());
            genericRecord.put("name", customer.getCustomerName());

            ProducerRecord<Integer, GenericRecord> record = new ProducerRecord<>("customers-avro", genericRecord);
            producer.send(record);
        }
    }

    public static void main(String[] args) {
        new Example();
    }
}
