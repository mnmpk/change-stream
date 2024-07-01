package com.example.demo.service;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import com.example.demo.model.ChangeStreamProcess;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

@Service
public class ChangeStreamService {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MongoTemplate mongoTemplate;

    private static List<ChangeStreamProcess<Document>> changeStreams;

    private static ChangeStreamProcess<Document> earilest = null;
    private static ChangeStreamProcess<Document> latest = null;

    public void splitRun(int noOfChangeStream, List<Bson> pipeline, boolean resume,
            Consumer<ChangeStreamDocument<Document>> body)
            throws Exception {
        if (changeStreams == null)
            changeStreams = new ArrayList<>();
        if (changeStreams.size() > 0)
            throw new Exception("Change stream is already running");
        for (int i = 0; i < noOfChangeStream; i++) {
            run(noOfChangeStream, i, pipeline, resume, body);
        }
    }

    public void run(int noOfChangeStream, int changeStreamIndex, List<Bson> pipeline, boolean resume,
            Consumer<ChangeStreamDocument<Document>> body) {
        ChangeStreamProcess<Document> process = new ChangeStreamProcess<Document>(noOfChangeStream, changeStreamIndex,
                pipeline, latest != null ? latest.getClusterTime() : null, body) {

            @Override
            public ChangeStreamIterable<Document> initChangeStream() {
                logger.info(getPipeline().toString());
                ChangeStreamIterable<Document> cs = mongoTemplate.getDb().watch(getPipeline());
                if (resume && earilest != null) {
                    logger.info(changeStreamIndex + "/" + noOfChangeStream + ": Resume Change stream from: "
                            + earilest.getClusterTime()/*+", resume token:"+earilest.getResumeToken()*/);
                    //cs.resumeAfter(earilest.getResumeToken());
                    cs.startAtOperationTime(earilest.getClusterTime());
                }
                return cs;
            }

        };
        changeStreams.add(process);
        logger.info(changeStreamIndex + "/" + noOfChangeStream + ": Run Change stream");
        new Thread(changeStreams.get(changeStreamIndex)).start();
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
