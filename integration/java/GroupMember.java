import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * A consumer group member using the official Java client with its default
 * group settings. It runs until killed and reports on standard output:
 *
 *   ASSIGNED <n>                     it now holds n partitions
 *   REC <partition> <offset> <value> one record
 *   COMMITTED                        everything printed so far is committed
 *
 * Usage: java -cp <kafka-clients jars> GroupMember.java <bootstrap> <topic> <group>
 */
public class GroupMember {
    public static void main(String[] args) {
        Properties p = new Properties();
        p.put("bootstrap.servers", args[0]);
        p.put("group.id", args[2]);
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("enable.auto.commit", "false");
        p.put("auto.offset.reset", "earliest");
        p.put("session.timeout.ms", "6000"); // so that a killed member is noticed quickly
        p.put("heartbeat.interval.ms", "500");
        // With one broker, a broker restart leaves no broker reachable, and the
        // default ("rebootstrap") then empties the client's cached metadata. The
        // group leader sees its topic vanish and rejoins: a client decision that
        // says nothing about the broker. See docs/BUGS.md, M7.
        p.put("metadata.recovery.strategy", "none");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
            consumer.subscribe(List.of(args[1]), new ConsumerRebalanceListener() {
                public void onPartitionsRevoked(Collection<TopicPartition> parts) {
                    System.out.println("ASSIGNED " + (consumer.assignment().size() - parts.size()));
                }

                public void onPartitionsAssigned(Collection<TopicPartition> parts) {
                    System.out.println("ASSIGNED " + consumer.assignment().size());
                }
            });
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, String> r : records) {
                    System.out.println("REC " + r.partition() + " " + r.offset() + " " + r.value());
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                    System.out.println("COMMITTED");
                }
            }
        }
    }
}
