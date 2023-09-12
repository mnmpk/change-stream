package com.example.demo.Controller;

import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
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
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;

@RestController
public class CSController {

    private Logger logger = LoggerFactory.getLogger(getClass());
    @Autowired
    private MongoTemplate mongoTemplate;

    @RequestMapping("/watch/{collection}")
    public void watch(@PathVariable("collection") String collection,
            @RequestParam(required = false, defaultValue = "10") int batchSize,
            @RequestParam(required = false, defaultValue = "10") int maxAwaitTime,
            @RequestParam(required = false, defaultValue = "0") int changeStreamIndex,
            @RequestParam(required = false, defaultValue = "1") int noOfChangeStream,
            @RequestParam(required = false, defaultValue = "false") boolean fullDocument) {
        watch(null, collection, batchSize, maxAwaitTime, changeStreamIndex, noOfChangeStream, fullDocument);
    }

    @RequestMapping("/watch/{resumeToken}/{collection}")
    public void watch(@PathVariable("resumeToken") String resumeTokenString,
            @PathVariable("collection") String collectionString,
            @RequestParam(required = false, defaultValue = "10") int batchSize,
            @RequestParam(required = false, defaultValue = "10") int maxAwaitTime,
            @RequestParam(required = false, defaultValue = "0") int changeStreamIndex,
            @RequestParam(required = false, defaultValue = "1") int noOfChangeStream,
            @RequestParam(required = false, defaultValue = "false") boolean fullDocument) {
        final String pipeline = """
				{
					$match:
					{
						$expr: {
						  $eq: [
							{
							  $abs: {
								$mod: [
								  {
									$toHashedIndexKey:
									  "$documentKey._id",
								  },
								""" + noOfChangeStream + """
									,
								],
							  },
							},
							""" + changeStreamIndex + """
								,
						  ],
						},
					  }
				  }
					""";
        CompletableFuture.runAsync(new Runnable() {
            @Override
            public void run() {
                MongoDatabase db = mongoTemplate.getDb();
                ChangeStreamIterable<Document> changeStream = null;
                if (StringUtils.hasText(collectionString)) {
                    MongoCollection<Document> collection = db.getCollection(collectionString, Document.class);
                    if (resumeTokenString != null) {
                        logger.info(db.getName() + "." + collectionString + " resume after: " + resumeTokenString);
                        BsonDocument resumeToken = new BsonDocument();
                        resumeToken.put("_data", new BsonString(resumeTokenString));
                        changeStream = collection.watch(List.of(Document.parse(pipeline))).resumeAfter(resumeToken);
                    } else {
                        logger.info(changeStreamIndex + ": Start watching " + db.getName() + "." + collectionString);
                        changeStream = collection.watch(List.of(Document.parse(pipeline)));
                    }
                } else {
                    if (resumeTokenString != null) {
                        logger.info(db.getName() + " resume after: " + resumeTokenString);
                        BsonDocument resumeToken = new BsonDocument();
                        resumeToken.put("_data", new BsonString(resumeTokenString));
                        changeStream = db.watch(List.of(Document.parse(pipeline)), Document.class).resumeAfter(resumeToken);
                    } else {
                        logger.info(changeStreamIndex + ": Start watching " + db.getName());
                        changeStream = db.watch(List.of(Document.parse(pipeline)), Document.class);
                    }
                }
                if (fullDocument) {
                    changeStream = changeStream.fullDocument(FullDocument.UPDATE_LOOKUP);
                }
                changeStream.batchSize(batchSize).maxAwaitTime(maxAwaitTime, TimeUnit.MILLISECONDS);

                changeStream.forEach(event -> {
                    try {
                        logger.info(changeStreamIndex + ": " + event.getOperationType().getValue() + " operation, resume token:" + event.getResumeToken().toJson());
                        Document doc = null;
                        switch (event.getOperationType()) {
                            case INSERT:
                                doc = event.getFullDocument();
                                //logger.info(doc.toJson());
                                //logger.info("Diff: " + (new Date().getTime() - new Date(doc.getDateTime("t").getValue()).getTime()+ "ms"));
                                if ("start".equalsIgnoreCase(doc.getString("i"))) {
                                    mongoTemplate.getCollection("timer").insertOne(new Document("t", new Date().getTime()));
                                } else if ("end".equalsIgnoreCase(doc.getString("i"))) {
									Long end = new Date().getTime();
                                    Long start = mongoTemplate.getCollection("timer").find().sort(Sorts.descending("t")).limit(1).first().getLong("t");
                                    int count = doc.getInteger("c");
									double diff = (end-start)/1000.0;
                                    logger.info(changeStreamIndex + ": No. of record inserted: " + count + " takes " + diff + "s, TPS:" + count / diff);
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
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                });
            }
        });

    }

    @RequestMapping("/watch-cursor/{collection}")
    public void watchCursor(@PathVariable("collection") String collection,
            @RequestParam(required = false, defaultValue = "10") int batchSize,
            @RequestParam(required = false, defaultValue = "10") int maxAwaitTime,
            @RequestParam(required = false, defaultValue = "0") int changeStreamIndex,
            @RequestParam(required = false, defaultValue = "1") int noOfChangeStream,
            @RequestParam(required = false, defaultValue = "false") boolean fullDocument) {
        watchCursor(null, collection, batchSize, maxAwaitTime, changeStreamIndex, noOfChangeStream, fullDocument);
    }

    @RequestMapping("/watch-cursor/{resumeToken}/{collection}")
    public void watchCursor(@PathVariable("resumeToken") String resumeTokenString,
            @PathVariable("collection") String collectionString,
            @RequestParam(required = false, defaultValue = "10") int batchSize,
            @RequestParam(required = false, defaultValue = "10") int maxAwaitTime,
            @RequestParam(required = false, defaultValue = "0") int changeStreamIndex,
            @RequestParam(required = false, defaultValue = "1") int noOfChangeStream,
            @RequestParam(required = false, defaultValue = "false") boolean fullDocument) {
        final String pipeline = """
				{
					$match:
					{
						$expr: {
						  $eq: [
							{
							  $abs: {
								$mod: [
								  {
									$toHashedIndexKey:
									  "$documentKey._id",
								  },
								""" + noOfChangeStream + """
									,
								],
							  },
							},
							""" + changeStreamIndex + """
								,
						  ],
						},
					  }
				  }
					""";
        CompletableFuture.runAsync(new Runnable() {
            @Override
            public void run() {
                MongoDatabase db = mongoTemplate.getDb();

                ChangeStreamIterable<Document> changeStream = null;
                if (StringUtils.hasText(collectionString)) {
                    MongoCollection<Document> collection = db.getCollection(collectionString, Document.class);
                    if (resumeTokenString != null) {
                        logger.info(db.getName() + "." + collectionString + " resume after: " + resumeTokenString);
                        BsonDocument resumeToken = new BsonDocument();
                        resumeToken.put("_data", new BsonString(resumeTokenString));
                        changeStream = collection.watch(List.of(Document.parse(pipeline))).resumeAfter(resumeToken);
                    } else {
                        logger.info(changeStreamIndex + ": Start watching " + db.getName() + "." + collectionString);
                        changeStream = collection.watch(List.of(Document.parse(pipeline)));
                    }
                } else {
                    if (resumeTokenString != null) {
                        logger.info(db.getName() + " resume after: " + resumeTokenString);
                        BsonDocument resumeToken = new BsonDocument();
                        resumeToken.put("_data", new BsonString(resumeTokenString));
                        changeStream = db.watch(List.of(Document.parse(pipeline)), Document.class).resumeAfter(resumeToken);
                    } else {
                        logger.info(changeStreamIndex + ": Start watching " + db.getName());
                        changeStream = db.watch(List.of(Document.parse(pipeline)), Document.class);
                    }
                }
                if (fullDocument) {
                    changeStream = changeStream.fullDocument(FullDocument.UPDATE_LOOKUP);
                }
                changeStream.batchSize(batchSize).maxAwaitTime(maxAwaitTime, TimeUnit.MILLISECONDS);
                MongoCursor<ChangeStreamDocument<Document>> a = changeStream.iterator();

                MongoChangeStreamCursor<ChangeStreamDocument<Document>> b = changeStream.cursor();
                //b.getResumeToken();

                try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = changeStream.cursor()) {
                    while (true) {
                        try {
                            ChangeStreamDocument<Document> event = cursor.tryNext();
                            if (event == null) {
                                continue;
                            }
                            logger.info(changeStreamIndex + ": " + event.getOperationType().getValue() + " operation, resume token:" + event.getResumeToken().toJson());
                            Document doc = null;
                            switch (event.getOperationType()) {
                                case INSERT:
                                    doc = event.getFullDocument();
                                    //logger.info(doc.toJson());
                                    //logger.info("Diff: " + (new Date().getTime() - new Date(doc.getDateTime("t").getValue()).getTime()+ "ms"));
									if ("start".equalsIgnoreCase(doc.getString("i"))) {
										mongoTemplate.getCollection("timer").insertOne(new Document("t", new Date().getTime()));
									} else if ("end".equalsIgnoreCase(doc.getString("i"))) {
										Long end = new Date().getTime();
										Long start = mongoTemplate.getCollection("timer").find().sort(Sorts.descending("t")).limit(1).first().getLong("t");
										int count = doc.getInteger("c");
										double diff = (end-start)/1000.0;
										logger.info(changeStreamIndex + ": No. of record inserted: " + count + " takes " + diff + "s, TPS:" + count / diff);
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
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                }
            }
        });
    }
}
