package com.example.demo.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

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
    private ChangeStreamProcessConfig<T> config;
    private BsonTimestamp lastEventAt;
    private boolean done = false;

    public ChangeStreamProcess(ChangeStreamProcessConfig<T> config,
            Consumer<ChangeStreamDocument<T>> body) {
        this.config = config;
        this.body = body;
    }

    public abstract ChangeStreamIterable<T> initChangeStream(List<Bson> pipeline);

    @Override
    public void run() {
        this.done = false;
        List<Bson> pipeline = new ArrayList<>();
        Document splitStage = new Document("$match",
                new Document("$expr",
                        new Document("$eq", Arrays.asList(new Document("$abs",
                                new Document("$mod", Arrays.asList(
                                        new Document("$toHashedIndexKey", "$documentKey._id"),
                                        config.getNoOfChangeStream()))),
                                config.getChangeStreamIndex()))));
        pipeline.add(splitStage);
        this.changeStream = initChangeStream(pipeline);

        if (config.getStartAt() != null) {
            logger.info(
                    config.getChangeStreamIndex() + "/" + config.getNoOfChangeStream() + ": Resume Change stream from: "
                            + config.getStartAt());
            this.changeStream.startAtOperationTime(config.getStartAt());
        }
        if (config.getEndAt() != null)
            this.lastEventAt = config.getEndAt();
        try (MongoChangeStreamCursor<ChangeStreamDocument<T>> cursor = this.changeStream.cursor()) {
            while (changeStream != null && active.get()) {
                ChangeStreamDocument<T> event = cursor.tryNext();
                if (event == null || (lastEventAt != null && lastEventAt.compareTo(event.getClusterTime()) >= 0)) {
                    if (event != null && lastEventAt != null) {
                        logger.info(
                                config.getChangeStreamIndex() + "/" + config.getNoOfChangeStream()
                                        + ": Skip event, cluster time:" + lastEventAt
                                        + ", event time:" + event.getClusterTime());
                    }
                    continue;
                }
                lastEventAt = event.getClusterTime();
                // resumeToken = event.getResumeToken();
                logger.info(
                        config.getChangeStreamIndex() + "/" + config.getNoOfChangeStream()
                                + ": Event received, cluster time:" + lastEventAt
                /* + ", resume token:" + resumeToken */);
                body.accept(event);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        this.done = true;
    }

    public void stop() {
        logger.info(config.getChangeStreamIndex() + "/" + config.getNoOfChangeStream() + ": Stop Change stream");
        active.set(false);
    }

    public BsonTimestamp getClusterTime() {
        return lastEventAt;
    }

    public boolean isDone() {
        return this.done;
    }
}
