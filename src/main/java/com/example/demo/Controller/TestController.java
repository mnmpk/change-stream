package com.example.demo.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.util.StopWatch;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mongodb.client.model.changestream.OperationType;

@RestController
public class TestController {

    private Logger logger = LoggerFactory.getLogger(getClass());
    @Autowired
    private MongoTemplate mongoTemplate;

	static StopWatch SW = new StopWatch();

    @RequestMapping("/watch2")
    public void watch2() {

        /*CreateCollectionOptions collectionOptions = new CreateCollectionOptions();
        collectionOptions.changeStreamPreAndPostImagesOptions(new ChangeStreamPreAndPostImagesOptions(true));
        mongoTemplate.getDb().createCollection("test_version", collectionOptions);*/
        CompletableFuture.runAsync(new Runnable() {
            @Override
            public void run() {
                MongoDatabase db = mongoTemplate.getDb();
                MongoCollection<Document> collection = db.getCollection("test_version", Document.class);
                collection.watch(List.of(
                        Aggregates.project(Projections.exclude("updateDescription.updatedFields.version")),
                        Aggregates.match(Filters.and(
                                Filters.in("operationType", List.of(OperationType.INSERT.getValue(), OperationType.UPDATE.getValue(), OperationType.REPLACE.getValue())),
                                Filters.not(
                                        Filters.eq("updateDescription.updatedFields", new Document())
                                ))
                        )
                )).fullDocument(FullDocument.UPDATE_LOOKUP).forEach(e -> {
                    logger.info("event:" + e);
                });
            }
        });
        CompletableFuture.runAsync(new Runnable() {
            @Override
            public void run() {
                try {//ChangeStreamDocument
                    MongoDatabase db = mongoTemplate.getDb();
                    MongoCollection<Document> collection = db.getCollection("test_version", Document.class);
                    collection.watch(List.of(
                            Aggregates.match(
                                    Filters.eq("operationType", OperationType.DELETE.getValue())
                            ),
                            Aggregates.project(Projections.include("ns", "operationType", "fullDocumentBeforeChange.test"))
                    )).fullDocumentBeforeChange(FullDocumentBeforeChange.REQUIRED).forEach(e -> {
                        logger.info("event:" + e);
                    });
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        });
    }

    @RequestMapping("/test")
    public void test() {
        List<Object> savePoList = new ArrayList<>();
        for (int i = 0; i < 100000; i++) {
            savePoList.add(new Object());
        }
        StopWatch sw = new StopWatch();
        sw.start();
        List<List<Object>> batches = IntStream.range(0, savePoList.size())
                .boxed()
                .collect(Collectors.groupingBy(index -> index / 1000))
                .values()
                .stream()
                .map(indices -> indices.stream().map(savePoList::get).collect(Collectors.toList()))
                .collect(Collectors.toList());
        sw.stop();
        logger.info("time used:" + sw.getTotalTimeMillis());
    }
}
