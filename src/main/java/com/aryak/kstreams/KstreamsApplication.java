package com.aryak.kstreams;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
public class KstreamsApplication {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private final Topology topology;

    public KstreamsApplication(final Topology topology) {
        this.topology = topology;
    }

    static void main(String[] args) {
        SpringApplication.run(KstreamsApplication.class, args);
    }

    @Bean
    public CommandLineRunner commandLineRunner() {
        return args -> {

            // create topics
            Properties adminProps = new Properties();
            adminProps.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

            //createTopics(adminProps);

            Properties props = new Properties();
            props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "app-id");
            props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
            kafkaStreams.start();
        };
    }

    private void createTopics(Properties adminProps) {
        // created topics programmatically first
        try ( AdminClient adminClient = AdminClient.create(adminProps) ) {
            List<NewTopic> topics = List.of(
                    new NewTopic("greetings", 1, (short) 1),
                    new NewTopic("greetings_uppercase", 1, (short) 1)
            );
            adminClient.createTopics(topics).all().get();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
