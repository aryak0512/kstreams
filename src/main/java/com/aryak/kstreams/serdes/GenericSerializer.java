package com.aryak.kstreams.serdes;

import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.databind.ObjectMapper;

public class GenericSerializer<T> implements Serializer<T> {

    private final ObjectMapper objectMapper;

    public GenericSerializer(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public byte[] serialize(final String s, final T t) {
        return objectMapper.writeValueAsBytes(t);
    }
}
