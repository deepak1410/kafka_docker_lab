package com.dpk.kafkadockerlab.controllers;

import com.dpk.kafkadockerlab.commands.CreatePeopleCommand;
import com.dpk.kafkadockerlab.entities.Person;
import com.github.javafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api")
public class PeopleController {

    private static final Logger logger = LoggerFactory.getLogger(PeopleController.class);

    @Value("${topics.people-basic.name}")
    String peopleBasicTopic;

    @Value("${topics.people-adv.name}")
    String peopleAdvTopic;

    private KafkaTemplate<String, Person> kafkaTemplate;

    public PeopleController(KafkaTemplate<String, Person> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/people-basic")
    @ResponseStatus(HttpStatus.CREATED)
    public List<Person> create(@RequestBody CreatePeopleCommand cmd) {
        logger.info("Create command is " + cmd);

        var faker = new Faker();
        List<Person> people = new ArrayList<>();

        for(var i = 0; i < cmd.getCount(); i++) {
            var person = new Person(
                    UUID.randomUUID().toString(),
                    faker.funnyName().name(),
                    faker.job().title()
            );

            people.add(person);
            kafkaTemplate.send(peopleBasicTopic, person.getTitle().toLowerCase().replaceAll("\\s+", "-"), person);
        }

        kafkaTemplate.flush();
        return people;
    }

    @PostMapping("/people-adv")
    @ResponseStatus(HttpStatus.CREATED)
    public List<Person> createAdv(@RequestBody CreatePeopleCommand cmd) {
        logger.info("Create command is " + cmd);

        var faker = new Faker();
        List<Person> people = new ArrayList<>();

        for(var i = 0; i < cmd.getCount(); i++) {
            var person = new Person(
                    UUID.randomUUID().toString(),
                    faker.funnyName().name(),
                    faker.job().title()
            );

            people.add(person);

            ListenableFuture<SendResult<String, Person>> future = kafkaTemplate.send(
                    peopleAdvTopic, person.getTitle().toLowerCase().replaceAll("\\s+", "-"), person);


            future.addCallback(
                    result -> {
                        logger.info("Published person=" + person
                        + "\n partition=" + result.getRecordMetadata().partition()
                        + "\n offset=" + result.getRecordMetadata().offset());
                    },
                    ex -> {
                        logger.error("Failed to publish " + person, ex);
                    }
            );


        }

        kafkaTemplate.flush();
        return people;
    }

}
