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

import static com.aryak.kstreams.util.Constants.GREETINGS;
import static com.aryak.kstreams.util.Constants.GREETINGS_UPPERCASE;

@Configuration
public class TopologyConfig {


    @Bean
    public Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // read
        KStream<String, Greeting> sourceStream = streamsBuilder.stream(GREETINGS, Consumed.with(Serdes.String(), new GreetingSerde()));

        // process
        KStream<String, Greeting> modifiedStream = sourceStream
                .mapValues((k, v) -> new Greeting(v.getMessage().toUpperCase(), v.getInstant()));

        // write
        modifiedStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), new GreetingSerde()));

        return streamsBuilder.build();
    }

}
