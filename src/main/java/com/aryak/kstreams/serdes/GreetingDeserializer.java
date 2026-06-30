package com.aryak.kstreams.serdes;

import com.aryak.kstreams.domain.Greeting;
import org.apache.kafka.common.serialization.Deserializer;
import tools.jackson.databind.ObjectMapper;

public class GreetingDeserializer implements Deserializer<Greeting> {

    private final ObjectMapper objectMapper;

    public GreetingDeserializer(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Greeting deserialize(final String s, final byte[] bytes) {
        return objectMapper.readValue(bytes, Greeting.class);
    }
}
