package com.example.demo;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.json.JSONObject;


public class KafkaBenchmark {
    public static final String INPUT_TOPIC = "change_stream.data";
    public static final String OUTPUT_TOPIC = "change_stream_result";

    static Properties getStreamsConfig(final String[] args) throws IOException {
        final Properties props = new Properties();
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "change_stream");
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.putIfAbsent(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    static Date d = null;

    static void createWordCountStream(final StreamsBuilder builder) {
        final KStream<String, String> source = builder.stream(INPUT_TOPIC);

        final KStream<String, String> counts = source.filter((k, v) -> {
            JSONObject obj = new JSONObject(v);
            JSONObject doc = new JSONObject(obj.getString("payload"));
            if (doc.has("fullDocument")) {
                JSONObject fd = doc.getJSONObject("fullDocument");
                if ("start".equalsIgnoreCase(fd.getString("i")) || "end".equalsIgnoreCase(fd.getString("i"))) {
                    return true;
                }
            }
            return false;
        }).mapValues(new ValueMapper<String, String>() {
            @Override
            public String apply(String value) {
                JSONObject obj = new JSONObject(value);
                JSONObject doc = new JSONObject(obj.getString("payload"));
                if (doc.has("fullDocument")) {
                    JSONObject fd = doc.getJSONObject("fullDocument");
                    if ("start".equalsIgnoreCase(fd.getString("i"))) {
                        d = new Date();
                    } else if ("end".equalsIgnoreCase(fd.getString("i"))) {
                        double diff = (new Date().getTime() - d.getTime())/1000d;                        
                        return "No. of record inserted: "+fd.getNumber("c").intValue()+" takes "+diff+"s, TPS:"+fd.getNumber("c").doubleValue()/diff;
                    }
                }
                return "Start recieving at " + d;
            }
        });

        // need to override value serde to Long type
        counts.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }

    public static void main(final String[] args) throws IOException {
        final Properties props = getStreamsConfig(args);

        final StreamsBuilder builder = new StreamsBuilder();
        createWordCountStream(builder);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
