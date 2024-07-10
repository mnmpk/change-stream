package com.example.demo.service;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.example.demo.model.ChangeStreamProcess;
import com.example.demo.model.ChangeStreamProcessConfig;

@Service
public class ChangeStreamService<T> {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private List<ChangeStreamProcess<T>> changeStreams = new ArrayList<>();

    private ChangeStreamProcess<T> earilest = null;
    private ChangeStreamProcess<T> latest = null;

    public void splitRun(int noOfChangeStream,
            Function<ChangeStreamProcessConfig<T>, ChangeStreamProcess<T>> initProcess)
            throws Exception {
        changeStreams.clear();
        if (changeStreams.size() > 0)
            throw new Exception("Change stream is already running");
        for (int i = 0; i < noOfChangeStream; i++) {
            run(noOfChangeStream, i, initProcess);
        }
    }

    @SuppressWarnings("unchecked")
    public void run(int noOfChangeStream, int changeStreamIndex,
            Function<ChangeStreamProcessConfig<T>, ChangeStreamProcess<T>> initProcess) {

        changeStreams.add(initProcess.apply((ChangeStreamProcessConfig<T>) ChangeStreamProcessConfig.builder()
                .startAt(earilest != null ? earilest.getClusterTime() : null)
                .endAt(latest != null ? latest.getClusterTime() : null)
                .noOfChangeStream(noOfChangeStream).changeStreamIndex(changeStreamIndex).build()));
        logger.info((changeStreamIndex+1) + "/" + noOfChangeStream + ": Run Change stream");
        new Thread(changeStreams.get(changeStreamIndex)).start();
    }

    public void stop() throws Exception {
        for (ChangeStreamProcess<T> process : changeStreams) {
            process.stop();
        }
        // Wait until all threads stopped completed
        while (!changeStreams.stream().allMatch(cs -> cs.isDone())) {
            Thread.sleep(100);
        }
        earilest = null;
        latest = null;
        for (ChangeStreamProcess<T> process : changeStreams) {
            if (earilest == null || earilest.getClusterTime().compareTo(process.getClusterTime()) > 0)
                earilest = process;
            if (latest == null || latest.getClusterTime().compareTo(process.getClusterTime()) < 0)
                latest = process;
        }
        logger.info("earilest:" + earilest.getClusterTime() + " latest:" + latest.getClusterTime());
        changeStreams.clear();
    }

    public void scaleRun(int noOfChangeStream,
            Function<ChangeStreamProcessConfig<T>, ChangeStreamProcess<T>> initProcess) throws Exception {
        if (changeStreams.size() > 0) {
            this.stop();
        }
        this.splitRun(noOfChangeStream, initProcess);
    }
}
