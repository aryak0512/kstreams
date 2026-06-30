package com.aryak.kstreams.serdes;

import com.aryak.kstreams.domain.Greeting;
import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.databind.ObjectMapper;

public class GreetingSerializer implements Serializer<Greeting> {

    private final ObjectMapper objectMapper;

    public GreetingSerializer(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public byte[] serialize(final String s, final Greeting greeting) {
        return objectMapper.writeValueAsBytes(greeting);
    }
}
