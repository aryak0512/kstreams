package com.aryak.kstreams.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import tools.jackson.databind.ObjectMapper;

public class GenericDeserializer<T> implements Deserializer<T> {

    private final ObjectMapper objectMapper;
    private final Class<T> destinationClass;

    public GenericDeserializer(final ObjectMapper objectMapper, final Class<T> destinationClass) {
        this.objectMapper = objectMapper;
        this.destinationClass = destinationClass;
    }

    @Override
    public T deserialize(final String s, final byte[] bytes) {
        return objectMapper.readValue(bytes, destinationClass);
    }
}
