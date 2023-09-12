package com.example.demo.service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;

@Component
public class AsyncService {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MongoTemplate mongoTemplate;

    @Async
    public CompletableFuture<Void> insert(int itemsPerThread, int index, String collection) throws InterruptedException {
        logger.info(Thread.currentThread().getName() + " start at: " + LocalDateTime.now().toString());
        List<WriteModel<Document>> bulkOperations = new ArrayList<WriteModel<Document>>();
        for (int i = 0; i < itemsPerThread; i++) {
            Document doc = new Document();
            doc.put("i", index + "-" + i);
            doc.put("t", new Date());
            bulkOperations.add(new InsertOneModel<>(doc));
        }
        StopWatch sw = new StopWatch();
        logger.info("start bulk write");
        sw.start();
        mongoTemplate.getCollection(collection).bulkWrite(bulkOperations);
        sw.stop();
        StringBuilder sb = new StringBuilder();
        sb.append(Thread.currentThread().getName());
        sb.append(" takes ");
        sb.append(sw.getTotalTimeSeconds());
        sb.append("s");

        logger.info(sb.toString());

        return CompletableFuture.completedFuture(null);
    }

    @Async
    public CompletableFuture<Void> insertOne(int itemsPerThread, int index, String collection) throws InterruptedException {
        logger.info(Thread.currentThread().getName() + " start at: " + LocalDateTime.now().toString());
        //List<WriteModel<Document>> bulkOperations = new ArrayList<WriteModel<Document>>();
        MongoCollection<Document> c = mongoTemplate.getCollection(collection);
        StopWatch sw = new StopWatch();
        sw.start();
        for (int i = 0; i < itemsPerThread; i++) {
            StopWatch sw2 = new StopWatch();
            sw2.start();
            Document doc = new Document();
            doc.put("i", index + "-" + i);
            doc.put("t", new Date());
            c.insertOne(doc);
            //logger.info("insert takes" + sw.getTotalTimeSeconds() + "s");
        }
        //logger.info("start bulk write");
        sw.stop();
        StringBuilder sb = new StringBuilder();
        sb.append(Thread.currentThread().getName());
        sb.append(" takes ");
        sb.append(sw.getTotalTimeSeconds());
        sb.append("s");

        logger.info(sb.toString());

        return CompletableFuture.completedFuture(null);
    }
}
