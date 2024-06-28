package com.example.demo.model;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

public class ChangeStreamProcess<T> implements Runnable {
    private AtomicBoolean active = new AtomicBoolean(true);

    private Consumer<ChangeStreamDocument<T>> body;
    private ChangeStreamIterable<T> changeStream;
    private BsonTimestamp clusterTime;
    private BsonDocument resumeToken;

    public ChangeStreamProcess(ChangeStreamIterable<T> changeStream, BsonTimestamp clusterTime,
            Consumer<ChangeStreamDocument<T>> body) {
        this.changeStream = changeStream;
        this.clusterTime = clusterTime;
        this.body = body;
    }

    @Override
    public void run() {
        while (active.get()) {
            ChangeStreamDocument<T> event = this.changeStream.cursor().tryNext();
            if (event == null || (clusterTime!=null && clusterTime.compareTo(event.getClusterTime())>=0)) {
                continue;
            }
            clusterTime = event.getClusterTime();
            resumeToken = event.getResumeToken();
            body.accept(event);
        }
    }

    public void stop() {
        active.set(false);
    }

    public BsonTimestamp getClusterTime() {
        return clusterTime;
    }

    public BsonDocument getResumeToken() {
        return resumeToken;
    }
}
