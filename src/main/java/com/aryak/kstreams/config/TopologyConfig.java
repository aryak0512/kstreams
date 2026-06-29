package com.aryak.kstreams.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TopologyConfig {

    private static final String sourceTopic = "greetings";

    private static final String destinationTopic = "greetings_uppercase";

    @Bean
    public Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // read
        KStream<String, String> sourceStream = streamsBuilder.stream(sourceTopic, Consumed.with(Serdes.String(), Serdes.String()));

        // process
        KStream<String, String> modifiedStream = sourceStream.mapValues((k, v) -> v.toUpperCase());

        // write
        modifiedStream.to(destinationTopic, Produced.with(Serdes.String(), Serdes.String()));

        return streamsBuilder.build();
    }

}
