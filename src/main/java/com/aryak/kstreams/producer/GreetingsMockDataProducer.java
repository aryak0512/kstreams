package com.aryak.kstreams.producer;


import com.aryak.kstreams.domain.Greeting;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.List;

import static com.aryak.kstreams.util.Constants.GREETINGS;
import static com.aryak.kstreams.util.ProducerUtil.publishMessageSync;


@Slf4j
public class GreetingsMockDataProducer {

    public static void spanishGreetings(ObjectMapper objectMapper) {
        var spanishGreetings = List.of(
                new Greeting("Hello, Good Morning!", LocalDateTime.now()),
                new Greeting("Hello, Good Evening!", LocalDateTime.now()),
                new Greeting("Hello, Good Night!", LocalDateTime.now())
        );
        spanishGreetings
                .forEach(greeting -> {
                    var greetingJSON = objectMapper.writeValueAsString(greeting);
                    var recordMetaData = publishMessageSync(GREETINGS, null, greetingJSON);
                    log.info("Published the alphabet message : {} ", recordMetaData);
                });
    }

    public static void englishGreetings(ObjectMapper objectMapper) {
        var spanishGreetings = List.of(
                new Greeting("¡Hola buenos dias!", LocalDateTime.now()),
                new Greeting("¡Hola buenas tardes!", LocalDateTime.now()),
                new Greeting("¡Hola, buenas noches!", LocalDateTime.now())
        );
        spanishGreetings
                .forEach(greeting -> {
                    var greetingJSON = objectMapper.writeValueAsString(greeting);
                    var recordMetaData = publishMessageSync(GREETINGS, null, greetingJSON);
                    log.info("Published the message : {} ", recordMetaData);
                });
    }

}

