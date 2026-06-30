package com.aryak.kstreams.serdes;

import com.aryak.kstreams.domain.Greeting;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.databind.ObjectMapper;

public class GreetingSerde implements Serde<Greeting> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Serializer<Greeting> serializer() {
        return new GreetingSerializer(objectMapper);
    }

    @Override
    public Deserializer<Greeting> deserializer() {
        return new GreetingDeserializer(objectMapper);
    }

}
