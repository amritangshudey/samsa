import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * Creates a topic, produces N records and reads them back with the official
 * Java client, printing each value read on its own line.
 *
 * Usage: java -cp <kafka-clients jars> RoundTrip.java <bootstrap> <topic> <n>
 */
public class RoundTrip {
    public static void main(String[] args) throws Exception {
        String bootstrap = args[0], topic = args[1];
        int n = Integer.parseInt(args[2]);

        Properties common = new Properties();
        common.put("bootstrap.servers", bootstrap);
        common.put("request.timeout.ms", "10000");
        common.put("default.api.timeout.ms", "20000");

        try (Admin admin = Admin.create(common)) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        }

        Properties pp = new Properties();
        pp.putAll(common);
        pp.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        pp.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        pp.put("acks", "all");
        // The broker does not implement the idempotent producer (a stated non-goal).
        pp.put("enable.idempotence", "false");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(pp)) {
            List<java.util.concurrent.Future<?>> sent = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                sent.add(producer.send(new ProducerRecord<>(topic, 0, "key-" + i, "java-" + i)));
            }
            for (var f : sent) {
                f.get();
            }
        }

        Properties cp = new Properties();
        cp.putAll(common);
        cp.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        cp.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        cp.put("enable.auto.commit", "false"); // no group: consumer groups arrive in Phase 4
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cp)) {
            TopicPartition tp = new TopicPartition(topic, 0);
            consumer.assign(List.of(tp));
            consumer.seekToBeginning(List.of(tp));
            int read = 0;
            long deadline = System.currentTimeMillis() + 30_000;
            while (read < n && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                    System.out.println(r.offset() + " " + r.value());
                    read++;
                }
            }
            if (read != n) {
                System.err.println("read " + read + " of " + n);
                System.exit(1);
            }
        }
    }
}
