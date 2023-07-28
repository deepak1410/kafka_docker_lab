package com.dpk.peopleconsumer.consumers;

import com.dpk.peopleconsumer.entities.Person;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PeopleConsumer {
    private static final Logger logger = LoggerFactory.getLogger(PeopleConsumer.class);
    
    @KafkaListener(topics = "${topics.people-basic.name}", containerFactory = "personBasicListenerFactory")
    public void handlePersonBasicEvent(Person person) {
        logger.info("Basic event Processing {}", person);
    }

    @KafkaListener(topics = "${topics.people-adv.name}", containerFactory = "personAdvListenerFactory")
    public void handlePersonAdvEvent(Person person) {
        logger.info("Advance event Processing {}", person);
    }

}
