package com.aryak.kstreams.config;

import com.aryak.kstreams.domain.Greeting;
import com.aryak.kstreams.serdes.GreetingSerde;
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
        KStream<Integer, Greeting> sourceStream = streamsBuilder.stream(sourceTopic, Consumed.with(Serdes.Integer(), new GreetingSerde()));

        // process
        KStream<Integer, Greeting> modifiedStream = sourceStream
                .mapValues((k, v) -> new Greeting(v.getMessage().toUpperCase(), v.getInstant()));

        // write
        modifiedStream.to(destinationTopic, Produced.with(Serdes.Integer(), new GreetingSerde()));

        return streamsBuilder.build();
    }

}
