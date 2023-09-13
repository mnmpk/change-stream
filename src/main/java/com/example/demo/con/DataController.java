package com.example.demo.con;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.bson.BsonDateTime;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.util.StopWatch;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo.service.AsyncService;

@RestController
public class DataController {

    private Logger logger = LoggerFactory.getLogger(getClass());
    @Autowired
    private MongoTemplate mongoTemplate;
    @Autowired
    private AsyncService service;

	static StopWatch SW = new StopWatch();

    @RequestMapping("/test/{collection}")
    public String test(@PathVariable("collection") String collectionString,
            @RequestParam(required = false, defaultValue = "true") boolean batch,
            @RequestParam(required = false, defaultValue = "20") int threads,
            @RequestParam(required = false, defaultValue = "1000") int itemsPerThread) {
        {
            try {
                logger.info("test start");
                Document d = new Document("test","test");

                List<CompletableFuture<Void>> ends = new ArrayList<CompletableFuture<Void>>();
                StopWatch sw = new StopWatch();
                Document doc = new Document();
                doc.put("i", "start");
                doc.put("t", new BsonDateTime(new Date().getTime()));
                mongoTemplate.getCollection(collectionString).insertOne(doc);
                sw.start();
                for (int i = 1; i <= threads; i++) {
                    if(batch)
                        ends.add(service.insert(itemsPerThread, i, collectionString, d));
                    else
                        ends.add(service.insertOne(itemsPerThread, i, collectionString, d));
                }
                CompletableFuture.allOf(ends.toArray(new CompletableFuture[ends.size()])).join();
                sw.stop();
                doc = new Document();
                doc.put("i", "end");
                doc.put("c", threads * itemsPerThread);
                doc.put("t", new Date());
                mongoTemplate.getCollection(collectionString).insertOne(doc);

                StringBuilder sb = new StringBuilder();
                sb.append("test() takes ");
                sb.append(sw.getTotalTimeSeconds());
                sb.append("s");
                logger.info("test end. " + sb.toString()+ ", TPS:"+(threads*itemsPerThread)/sw.getTotalTimeSeconds());
                return sb.toString();
            } catch (Exception ex) {
                return ex.toString();
            }
        }
    }
}
