import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.errors.WakeupException;

// ShareMember runs one Java 4.4.0 KafkaShareConsumer against a bootstrap
// server, printing READY, REC/REL lines, and COMMITTED as records are
// acknowledged, until stdin closes.
public final class ShareMember {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("usage: ShareMember <bootstrap> <group> <topic> [releaseFirstN]");
            System.exit(2);
        }
        String bootstrap = args[0];
        String group = args[1];
        String topic = args[2];
        int releaseFirstN = args.length >= 4 ? Integer.parseInt(args[3]) : 0;

        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("group.id", group);
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("share.acknowledgement.mode", "explicit");

        AtomicBoolean running = new AtomicBoolean(true);
        try (KafkaShareConsumer<String, String> c = new KafkaShareConsumer<>(p)) {
            Thread watcher = new Thread(() -> {
                try {
                    while (System.in.read() != -1) {
                    }
                } catch (Exception ignored) {
                }
                running.set(false);
                c.wakeup();
            });
            watcher.setDaemon(true);
            watcher.start();

            c.subscribe(Collections.singletonList(topic));
            boolean ready = false;
            int released = 0;
            while (running.get()) {
                ConsumerRecords<String, String> recs;
                try {
                    recs = c.poll(Duration.ofMillis(200));
                } catch (WakeupException e) {
                    if (!running.get()) {
                        break;
                    }
                    continue;
                }
                if (!ready) {
                    System.out.println("READY");
                    System.out.flush();
                    ready = true;
                }
                if (recs.isEmpty()) {
                    continue;
                }
                for (ConsumerRecord<String, String> r : recs) {
                    if (released < releaseFirstN) {
                        c.acknowledge(r, AcknowledgeType.RELEASE);
                        released++;
                        System.out.println("REL " + r.partition() + " " + r.offset() + " " + r.value());
                    } else {
                        c.acknowledge(r, AcknowledgeType.ACCEPT);
                        System.out.println("REC " + r.partition() + " " + r.offset() + " " + r.value());
                    }
                }
                c.commitSync();
                System.out.println("COMMITTED");
                System.out.flush();
            }
        }
    }
}
