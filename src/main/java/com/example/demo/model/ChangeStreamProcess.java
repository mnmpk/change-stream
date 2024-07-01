package com.example.demo.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

public abstract class ChangeStreamProcess<T> implements Runnable {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private AtomicBoolean active = new AtomicBoolean(true);

    private Consumer<ChangeStreamDocument<T>> body;
    private ChangeStreamIterable<T> changeStream;
    private BsonTimestamp clusterTime;
    //private BsonDocument resumeToken;
    private List<Bson> pipeline;
    private int noOfChangeStream;
    private int changeStreamIndex;

    public ChangeStreamProcess(int noOfChangeStream, int changeStreamIndex, List<Bson> pipeline,
            BsonTimestamp clusterTime,
            Consumer<ChangeStreamDocument<T>> body) {
        this.noOfChangeStream = noOfChangeStream;
        this.changeStreamIndex = changeStreamIndex;
        this.clusterTime = clusterTime;
        this.body = body;
        this.setPipeline(pipeline);
    }

    public abstract ChangeStreamIterable<T> initChangeStream();

    @Override
    public void run() {

        this.changeStream = initChangeStream();
        try (MongoChangeStreamCursor<ChangeStreamDocument<T>> cursor = this.changeStream.cursor()) {
            while (changeStream != null && active.get()) {
                ChangeStreamDocument<T> event = cursor.tryNext();
                if (event == null || (clusterTime != null && clusterTime.compareTo(event.getClusterTime()) >= 0)) {
                    if (event != null && clusterTime != null) {
                        logger.info(
                                changeStreamIndex + "/" + noOfChangeStream + ": Skip event, cluster time:" + clusterTime
                                        + ", event time:" + event.getClusterTime());
                    }
                    continue;
                }
                clusterTime = event.getClusterTime();
                //resumeToken = event.getResumeToken();
                logger.info(
                        changeStreamIndex + "/" + noOfChangeStream + ": Event received, cluster time:" + clusterTime
                                /*+ ", resume token:" + resumeToken*/);
                body.accept(event);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public List<Bson> getPipeline() {
        return this.pipeline;
    }

    public void setPipeline(List<Bson> p) {
        this.pipeline = new ArrayList<>();

        Document splitStage = new Document("$match",
                new Document("$expr",
                        new Document("$eq", Arrays.asList(new Document("$abs",
                                new Document("$mod", Arrays.asList(
                                        new Document("$toHashedIndexKey", "$documentKey._id"), noOfChangeStream))),
                                changeStreamIndex))));
        this.pipeline.add(splitStage);
        if (p != null && p.size() > 0)
            this.pipeline.addAll(p);
    }

    public void stop() {
        logger.info(changeStreamIndex + "/" + noOfChangeStream + ": Stop Change stream");
        active.set(false);
    }

    public BsonTimestamp getClusterTime() {
        return clusterTime;
    }

    /*public BsonDocument getResumeToken() {
        return resumeToken;
    }*/
}
