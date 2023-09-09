package com.example.demo;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.util.StopWatch;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mongodb.client.model.changestream.OperationType;

import lombok.var;

@RestController
public class TestController {

    private Logger logger = LoggerFactory.getLogger(getClass());
    @Autowired
    private MongoTemplate mongoTemplate;
    @Autowired
    private AsyncService service;
    private int batchSize = 10000;
    private int maxAwaitTime = 1000;
    private int noOfChangeStream = 3;

	static StopWatch SW = new StopWatch();

    @RequestMapping("/test/{collection}")
    public String test(@PathVariable("collection") String collectionString,
            @RequestParam(required = false, defaultValue = "20") int threads,
            @RequestParam(required = false, defaultValue = "1000") int itemsPerThread) {
        {
            try {
                logger.info("test start");

                var ends = new ArrayList<CompletableFuture<Void>>();
                var sw = new StopWatch();
                sw.start();
                Document doc = new Document();
                doc.put("i", "start");
                doc.put("t", new BsonDateTime(new Date().getTime()));
                mongoTemplate.getCollection(collectionString).insertOne(doc);
                for (int i = 1; i <= threads; i++) {
                    //ends.add(service.insert(itemsPerThread, i, collectionString));
                    ends.add(service.insertOne(itemsPerThread, i, collectionString));
                }
                CompletableFuture.allOf(ends.toArray(new CompletableFuture[ends.size()])).join();
                sw.stop();
                doc = new Document();
                doc.put("i", "end");
                doc.put("c", threads * itemsPerThread);
                doc.put("t", new Date());
                mongoTemplate.getCollection(collectionString).insertOne(doc);

                var sb = new StringBuilder();
                sb.append("test() takes ");
                sb.append(sw.getTotalTimeSeconds());
                sb.append("s");
                logger.info("test end. " + sb.toString());
                return sb.toString();
            } catch (Exception ex) {
                return ex.toString();
            }
        }
    }

    @RequestMapping("/watch")
    public void watch() {
        watch(null, null, false);
    }

    @RequestMapping("/watch/{collection}")
    public void watch(@PathVariable("collection") String collection,
            @RequestParam(required = false, defaultValue = "false") boolean fullDocument) {
        watch(null, collection, fullDocument);
    }

    @RequestMapping("/watch/{resumeToken}/{collection}")
    public void watch(@PathVariable("resumeToken") String resumeTokenString,
            @PathVariable("collection") String collectionString,
            @RequestParam(required = false, defaultValue = "false") boolean fullDocument) {

        for (int i = 0; i < noOfChangeStream; i++) {
            final int changeStreamIndex = i;
			final String pipeline = """
				{
					$match:
					  {
						$expr: {
						  $eq: [
							{
							  $mod: [
								{
								  $divide: [
									{
									  $toLong: {
										$toDate: "$documentKey._id",
									  },
									},
									1000,
								  ],
								},
								"""+noOfChangeStream+"""
									,
							  ],
							},
							"""+changeStreamIndex+"""
								,
						  ],
						},
					  },
				  }
					""";
            CompletableFuture.runAsync(new Runnable() {
                @Override
                public void run() {
                    MongoDatabase db = mongoTemplate.getDb();
                    ChangeStreamIterable<RawBsonDocument> changeStream = null;
                    if (StringUtils.hasText(collectionString)) {
                        MongoCollection<RawBsonDocument> collection = db.getCollection(collectionString, RawBsonDocument.class);
                        if (resumeTokenString != null) {
                            logger.info(db.getName() + "." + collectionString + " resume after: " + resumeTokenString);
                            BsonDocument resumeToken = new BsonDocument();
                            resumeToken.put("_data", new BsonString(resumeTokenString));
                            changeStream = collection.watch(List.of(Document.parse(pipeline))).resumeAfter(resumeToken);
                        } else {
                            logger.info(changeStreamIndex+": Start watching " + db.getName() + "." + collectionString);
                            changeStream = collection.watch(List.of(Document.parse(pipeline)));
                        }
                    } else {
                        if (resumeTokenString != null) {
                            logger.info(db.getName() + " resume after: " + resumeTokenString);
                            BsonDocument resumeToken = new BsonDocument();
                            resumeToken.put("_data", new BsonString(resumeTokenString));
                            changeStream = db.watch(List.of(Document.parse(pipeline)), RawBsonDocument.class).resumeAfter(resumeToken);
                        } else {
                            logger.info(changeStreamIndex+": Start watching " + db.getName());
                            changeStream = db.watch(List.of(Document.parse(pipeline)), RawBsonDocument.class);
                        }
                    }
                    if (fullDocument) {
                        changeStream = changeStream.fullDocument(FullDocument.UPDATE_LOOKUP);
                    }
                    changeStream.batchSize(batchSize).maxAwaitTime(maxAwaitTime, TimeUnit.MILLISECONDS);


                    changeStream.forEach(event -> {
						try{
							logger.info(changeStreamIndex+": "+event.getOperationType().getValue() + " operation, resume token:" + event.getResumeToken().toJson());
							RawBsonDocument doc = null;
							switch (event.getOperationType()) {
								case INSERT:
									doc = event.getFullDocument();
									//logger.info(doc.toJson());
									//logger.info("Diff: " + (new Date().getTime() - new Date(doc.getDateTime("t").getValue()).getTime()+ "ms"));
									if ("start".equalsIgnoreCase(doc.getString("i").getValue())) {
										SW = new StopWatch();
										SW.start();
									} else if ("end".equalsIgnoreCase(doc.getString("i").getValue())) {
										SW.stop();
										int count = doc.getInt32("c").getValue();
										logger.info(changeStreamIndex+": No. of record inserted: " + count + " takes " + SW.getTotalTimeSeconds() + "s, TPS:" + count / SW.getTotalTimeSeconds());
									}
									break;
								case UPDATE:
									if (fullDocument) {
										doc = event.getFullDocument();
										logger.info(doc.toJson());
									}
									logger.info(event.getUpdateDescription().toString());
									break;
								default:
									break;
							}
						}catch(Exception ex){
							ex.printStackTrace();
						}
                    });
                }
            });
        }

    }

    @RequestMapping("/watch-cursor")
    public void watchCursor() {
        watch(null, null, false);
    }

    @RequestMapping("/watch-cursor/{collection}")
    public void watchCursor(@PathVariable("collection") String collection,
            @RequestParam(required = false, defaultValue = "false") boolean fullDocument) {
        watch(null, collection, fullDocument);
    }

    @RequestMapping("/watch-cursor/{resumeToken}/{collection}")
    public void watchCursor(@PathVariable("resumeToken") String resumeTokenString,
            @PathVariable("collection") String collectionString,
            @RequestParam(required = false, defaultValue = "false") boolean fullDocument) {

        for (int i = 0; i < noOfChangeStream; i++) {
            final int changeStreamIndex = i;
			final String pipeline = """
				{
					$match:
					  {
						$expr: {
						  $eq: [
							{
							  $mod: [
								{
								  $divide: [
									{
									  $toLong: {
										$toDate: "$$documentKey._id",
									  },
									},
									1000,
								  ],
								},
								"""+noOfChangeStream+"""
									,
							  ],
							},
							"""+changeStreamIndex+"""
								,
						  ],
						},
					  },
				  }
					""";
            CompletableFuture.runAsync(new Runnable() {
                @Override
                public void run() {
                    MongoDatabase db = mongoTemplate.getDb();

                    ChangeStreamIterable<RawBsonDocument> changeStream = null;
                    if (StringUtils.hasText(collectionString)) {
                        MongoCollection<RawBsonDocument> collection = db.getCollection(collectionString, RawBsonDocument.class);
                        if (resumeTokenString != null) {
                            logger.info(db.getName() + "." + collectionString + " resume after: " + resumeTokenString);
                            BsonDocument resumeToken = new BsonDocument();
                            resumeToken.put("_data", new BsonString(resumeTokenString));
                            changeStream = collection.watch(List.of(Document.parse(pipeline))).resumeAfter(resumeToken);
                        } else {
                            logger.info(changeStreamIndex+": Start watching " + db.getName() + "." + collectionString);
                            changeStream = collection.watch(List.of(Document.parse(pipeline)));
                        }
                    } else {
                        if (resumeTokenString != null) {
                            logger.info(db.getName() + " resume after: " + resumeTokenString);
                            BsonDocument resumeToken = new BsonDocument();
                            resumeToken.put("_data", new BsonString(resumeTokenString));
                            changeStream = db.watch(List.of(Document.parse(pipeline)), RawBsonDocument.class).resumeAfter(resumeToken);
                        } else {
                            logger.info(changeStreamIndex+": Start watching " + db.getName());
                            changeStream = db.watch(List.of(Document.parse(pipeline)), RawBsonDocument.class);
                        }
                    }
                    if (fullDocument) {
                        changeStream = changeStream.fullDocument(FullDocument.UPDATE_LOOKUP);
                    }
                    changeStream.batchSize(batchSize).maxAwaitTime(maxAwaitTime, TimeUnit.MILLISECONDS);
                    MongoCursor<ChangeStreamDocument<RawBsonDocument>> a = changeStream.iterator();

                    MongoChangeStreamCursor<ChangeStreamDocument<RawBsonDocument>> b = changeStream.cursor();

                    try (MongoChangeStreamCursor<ChangeStreamDocument<RawBsonDocument>> cursor = changeStream.cursor()) {
                        while (true) {
							try{
								ChangeStreamDocument<RawBsonDocument> event = cursor.tryNext();
								if (event == null) {
									continue;
								}
								logger.info(changeStreamIndex+": "+event.getOperationType().getValue() + " operation, resume token:" + event.getResumeToken().toJson());
								RawBsonDocument doc = null;
								switch (event.getOperationType()) {
									case INSERT:
										doc = event.getFullDocument();
										//logger.info(doc.toJson());
										//logger.info("Diff: " + (new Date().getTime() - new Date(doc.getDateTime("t").getValue()).getTime()+ "ms"));
										if ("start".equalsIgnoreCase(doc.getString("i").getValue())) {
											SW = new StopWatch();
											SW.start();
										} else if ("end".equalsIgnoreCase(doc.getString("i").getValue())) {
											SW.stop();
											int count = doc.getInt32("c").getValue();
											logger.info(changeStreamIndex+": No. of record inserted: " + count + " takes " + SW.getTotalTimeSeconds() + "s, TPS:" + count / SW.getTotalTimeSeconds());
										}
										break;
									case UPDATE:
										if (fullDocument) {
											doc = event.getFullDocument();
											logger.info(doc.toJson());
										}
										logger.info(event.getUpdateDescription().toString());
										break;
									default:
										break;
								}
							}catch(Exception ex){
								ex.printStackTrace();
							}
                        }
                    }
                }
            });
        }
    }

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
                .collect(Collectors.groupingBy(index -> index / batchSize))
                .values()
                .stream()
                .map(indices -> indices.stream().map(savePoList::get).collect(Collectors.toList()))
                .collect(Collectors.toList());
        sw.stop();
        logger.info("time used:" + sw.getTotalTimeMillis());
    }
}
