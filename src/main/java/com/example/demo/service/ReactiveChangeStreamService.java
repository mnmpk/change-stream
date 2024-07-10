package com.example.demo.service;

import java.util.List;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoDatabase;

@Service
public class ReactiveChangeStreamService {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Value("${spring.data.mongodb.uri}")
    private String uri;

    public void run(String collection) {

        MongoClient mongoClient = MongoClients.create(uri);

        MongoDatabase database = mongoClient.getDatabase("change-stream");
        database.watch(List.of(Aggregates.match(
                Filters.in("ns.coll", List.of(collection)))))
                .subscribe(new ChangeStreamSubscriber<Document>((ChangeStreamDocument<Document> csd) -> {
                    logger.info("event received:" + csd);
                }, (t) -> {
                    logger.error("error", t);
                }));

    }
}
