package com.example.demo;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.CompletableFuture;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.util.StopWatch;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.changestream.FullDocument;

import lombok.var;

@RestController
public class TestController {

	private Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	private MongoTemplate mongoTemplate;

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
				doc.put("t", new Date());
				mongoTemplate.getCollection(collectionString).insertOne(doc);
				for (int i = 1; i <= threads; i++) {
					ends.add(insert(itemsPerThread, i, collectionString));
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

	@Async
	public CompletableFuture<Void> insert(int itemsPerThread, int index, String collection) throws InterruptedException {
		logger.info(Thread.currentThread().getName() + " start at: " + LocalDateTime.now().toString());
		var bulkOperations = new ArrayList<WriteModel<Document>>();
		for (int i = 0; i < itemsPerThread; i++) {
			Document doc = new Document();
			doc.put("i", index + "-" + i);
			doc.put("t", new Date());
			bulkOperations.add(new InsertOneModel<>(doc));
		}
		var sw = new StopWatch();
		logger.info("start bulk write");
		sw.start();
		mongoTemplate.getCollection(collection).bulkWrite(bulkOperations);
		sw.stop();
		var sb = new StringBuilder();
		sb.append(Thread.currentThread().getName());
		sb.append(" takes ");
		sb.append(sw.getTotalTimeSeconds());
		sb.append("s");

		logger.info(sb.toString());

		return CompletableFuture.completedFuture(null);
	}
	@Async
	public CompletableFuture<Void> insertOne(int index, String collection) throws InterruptedException {
		logger.info(Thread.currentThread().getName() + " start at: " + LocalDateTime.now().toString());
		//var bulkOperations = new ArrayList<WriteModel<Document>>();
		var c = mongoTemplate.getCollection(collection);
		var sw = new StopWatch();
		sw.start();
		for (int i = 0; i < 10; i++) {
			var sw2 = new StopWatch();
		sw2.start();
			Document doc = new Document();
			doc.put("i", index + "-" + i);
			doc.put("t", new Date());
			c.insertOne(doc);
			logger.info("insert takes"+sw.getTotalTimeSeconds()+"s");
		}
		//logger.info("start bulk write");
		sw.stop();
		var sb = new StringBuilder();
		sb.append(Thread.currentThread().getName());
		sb.append(" takes ");
		sb.append(sw.getTotalTimeSeconds());
		sb.append("s");

		logger.info(sb.toString());

		return CompletableFuture.completedFuture(null);
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
				ChangeStreamIterable<Document> changeStream = null;
				if (StringUtils.hasText(collectionString)) {
					MongoCollection<Document> collection = mongoTemplate.getCollection(collectionString);
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
						changeStream = db.watch().resumeAfter(resumeToken);
					} else {
						logger.info("Start watching " + db.getName());
						changeStream = db.watch();
					}
				}
				if (fullDocument) {
					changeStream = changeStream.fullDocument(FullDocument.UPDATE_LOOKUP);
				}
				changeStream.batchSize(100);
				var sw = new StopWatch();
				changeStream.forEach(event -> {
					logger.info(event.getOperationType().getValue() + " operation, resume token:" + event.getResumeToken().toJson());
					Document doc = null;
					switch (event.getOperationType()) {
						case INSERT:
							doc = event.getFullDocument();
							logger.info(doc.toJson());
							logger.info("Diff: " + (new Date().getTime() - doc.getDate("t").getTime() + "ms"));
							if("start".equalsIgnoreCase(doc.getString("i"))){
								sw.start();
							}else if("end".equalsIgnoreCase(doc.getString("i"))){
								sw.stop();
								int count = doc.getInteger("c");
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
				});
			}
		});
	}

}
