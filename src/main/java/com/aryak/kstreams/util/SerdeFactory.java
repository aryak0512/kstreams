package com.aryak.kstreams.util;

import com.aryak.kstreams.serdes.GenericDeserializer;
import com.aryak.kstreams.serdes.GenericSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import tools.jackson.databind.ObjectMapper;

public class SerdeFactory {

    private SerdeFactory() {

    }

    public static <T> Serde<T> get(Class<T> target) {
        GenericSerializer<T> serializer = new GenericSerializer<>(new ObjectMapper());
        GenericDeserializer<T> deserializer = new GenericDeserializer<>(new ObjectMapper(), target);
        return Serdes.serdeFrom(serializer, deserializer);
    }

}
