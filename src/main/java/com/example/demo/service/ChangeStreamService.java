package com.example.demo.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import com.example.demo.model.ChangeStreamProcess;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

@Service
public class ChangeStreamService<T> {

    @Autowired
    private MongoTemplate mongoTemplate;

    private static List<ChangeStreamProcess<Document>> changeStreams;

    private static ChangeStreamProcess<Document> earilest = null;
    private static ChangeStreamProcess<Document> latest = null;

    // TODO: allow use custom class
    public void splitRun(int noOfChangeStream, List<Document> pipeline, Consumer<ChangeStreamDocument<Document>> body)
            throws Exception {
        if (changeStreams.size() > 0)
            throw new Exception("Change stream is already running");
        for (int i = 0; i < noOfChangeStream; i++) {
            run(noOfChangeStream, i, pipeline, body);
        }
    }

    public List<Document> splitStage(int noOfChangeStream, int changeStreamIndex, List<Document> pipeline) {
        List<Document> p = new ArrayList<>();
        p.addAll(pipeline);
        p.add(new Document("$match",
                new Document("$and", new Document("$expr",
                        new Document("$eq", Arrays.asList(new Document("$abs",
                                new Document("$mod", Arrays.asList(
                                        new Document("$toHashedIndexKey", "$documentKey._id"), noOfChangeStream))),
                                changeStreamIndex))))));
        return p;

    }

    public void run(int noOfChangeStream, int changeStreamIndex, List<Document> pipeline,
            Consumer<ChangeStreamDocument<Document>> body) {
        // TODO: allow watch both DB and coll
        ChangeStreamIterable<Document> changeStream = mongoTemplate.getDb()
                .watch(splitStage(noOfChangeStream, changeStreamIndex, pipeline), Document.class);
        if (earilest != null) {
            changeStream.resumeAfter(earilest.getResumeToken());
        }
        changeStreams.add(new ChangeStreamProcess<Document>(changeStream, latest.getClusterTime(), body));
        changeStreams.get(changeStreamIndex).run();
    }

    public void stop() {
        for (ChangeStreamProcess<Document> process : changeStreams) {
            process.stop();
            if (earilest == null || earilest.getClusterTime().compareTo(process.getClusterTime()) > 0)
                earilest = process;
            if (latest == null || latest.getClusterTime().compareTo(process.getClusterTime()) < 0)
                latest = process;
        }
        changeStreams.clear();
    }
}
