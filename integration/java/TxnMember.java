import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

// TxnMember exercises Java 4.4.0 transactional producer (abort, commit,
// sendOffsetsToTransaction, KIP-890 EndTxn v5) and read_committed consumer.
public final class TxnMember {
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("usage: TxnMember <bootstrap> <topic> <txnID> <group>");
            System.exit(2);
        }
        String bootstrap = args[0];
        String topic = args[1];
        String txnID = args[2];
        String group = args[3];

        Properties pp = new Properties();
        pp.put("bootstrap.servers", bootstrap);
        pp.put("transactional.id", txnID);
        pp.put("enable.idempotence", "true");
        pp.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        pp.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> p = new KafkaProducer<>(pp)) {
            p.initTransactions();

            p.beginTransaction();
            p.send(new ProducerRecord<>(topic, "k", "abort-0")).get();
            p.send(new ProducerRecord<>(topic, "k", "abort-1")).get();
            p.abortTransaction();

            p.beginTransaction();
            p.send(new ProducerRecord<>(topic, "k", "commit-0")).get();
            p.send(new ProducerRecord<>(topic, "k", "commit-1")).get();
            p.commitTransaction();

            // Third transaction exercises KIP-890 epoch bump + sendOffsetsToTransaction.
            p.beginTransaction();
            p.send(new ProducerRecord<>(topic, "k", "commit-2")).get();
            p.sendOffsetsToTransaction(
                Map.of(new TopicPartition(topic, 0), new OffsetAndMetadata(5L)),
                new ConsumerGroupMetadata(group));
            p.commitTransaction();
        }

        Properties cp = new Properties();
        cp.put("bootstrap.servers", bootstrap);
        cp.put("group.id", group + "-rc");
        cp.put("isolation.level", "read_committed");
        cp.put("auto.offset.reset", "earliest");
        cp.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        cp.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        List<String> got = new ArrayList<>();
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(cp)) {
            c.assign(Collections.singletonList(new TopicPartition(topic, 0)));
            long deadline = System.currentTimeMillis() + 15000;
            while (got.size() < 3 && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> recs = c.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, String> r : recs) {
                    if (r.value().startsWith("abort-")) {
                        throw new AssertionError("read_committed saw aborted record: " + r.value());
                    }
                    got.add(r.value());
                }
            }
        }
        if (!got.equals(List.of("commit-0", "commit-1", "commit-2"))) {
            throw new AssertionError("unexpected records: " + got);
        }
        System.out.println("OK " + got.size());
    }
}
