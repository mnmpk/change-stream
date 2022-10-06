package com.example.demo;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.bson.BsonBinaryReader;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonType;
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
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;

import lombok.var;

@RestController
public class TestController {

	private Logger logger = LoggerFactory.getLogger(getClass());
	@Autowired
	private MongoTemplate mongoTemplate;
	@Autowired
	private AsyncService service;
	
	private ArrayBlockingQueue<BsonDocument> queue;

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
					ends.add(service.insert(itemsPerThread, i, collectionString));
					//ends.add(insertOne(i, collectionString));
				}
				CompletableFuture.allOf(ends.toArray(new CompletableFuture[ends.size()])).join();
				sw.stop();
				doc = new Document();
				doc.put("i", "end");
				doc.put("c", threads*itemsPerThread);
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
						changeStream = collection.watch().resumeAfter(resumeToken);
					} else {
						logger.info("Start watching " + db.getName() + "." + collectionString);
						changeStream = collection.watch();
					}
				} else {
					if (resumeTokenString != null) {
						logger.info(db.getName() + " resume after: " + resumeTokenString);
						BsonDocument resumeToken = new BsonDocument();
						resumeToken.put("_data", new BsonString(resumeTokenString));
						changeStream = db.watch(RawBsonDocument.class).resumeAfter(resumeToken);
					} else {
						logger.info("Start watching " + db.getName());
						changeStream = db.watch(RawBsonDocument.class);
					}
				}
				if (fullDocument) {
					changeStream = changeStream.fullDocument(FullDocument.UPDATE_LOOKUP);
				}
				changeStream.batchSize(500).maxAwaitTime(500, TimeUnit.MILLISECONDS);
				var sw = new StopWatch();
				
				try (MongoChangeStreamCursor<ChangeStreamDocument<RawBsonDocument>> cursor = changeStream.cursor()) {
					while (true) {
						ChangeStreamDocument<RawBsonDocument> event = cursor.tryNext();
						if(event==null)
							continue;
						logger.info(event.getOperationType().getValue() + " operation, resume token:" + event.getResumeToken().toJson());
						/*RawBsonDocument doc = null;
						switch (event.getOperationType()) {
							case INSERT:
								doc = event.getFullDocument();
								logger.info(doc.toJson());
								logger.info("Diff: " + (new Date().getTime() - new Date(doc.getDateTime("t").getValue()).getTime()+ "ms"));
								if("start".equalsIgnoreCase(doc.getString("i").getValue())){
									sw.start();
								}else if("end".equalsIgnoreCase(doc.getString("i").getValue())){
									sw.stop();
									int count = doc.getInt32("c").getValue();
									logger.info("No. of record inserted: "+count+" takes "+sw.getTotalTimeSeconds()+"s, TPS:"+count/sw.getTotalTimeSeconds());
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
						}*/
					}
				}
				
				/* changeStream.forEach(event -> {
					logger.info(event.getOperationType().getValue() + " operation, resume token:" + event.getResumeToken().toJson());
					RawBsonDocument doc = null;
					switch (event.getOperationType()) {
						case INSERT:
							doc = event.getFullDocument();
							logger.info(doc.toJson());
							logger.info("Diff: " + (new Date().getTime() - new Date(doc.getDateTime("t").getValue()).getTime()+ "ms"));
							if("start".equalsIgnoreCase(doc.getString("i").getValue())){
								sw.start();
							}else if("end".equalsIgnoreCase(doc.getString("i").getValue())){
								sw.stop();
								int count = doc.getInt32("c").getValue();
								logger.info("No. of record inserted: "+count+" takes "+sw.getTotalTimeSeconds()+"s, TPS:"+count/sw.getTotalTimeSeconds());
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
				});*/
			}
		});
	}
}
