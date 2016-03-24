package com.palantir.leader;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.palantir.leader.LeaderElectionService.LeadershipToken;
import com.palantir.leader.LeaderElectionService.StillLeadingStatus;

public class PaxosRequestBatcher {
    private static final Logger log = LoggerFactory.getLogger(PaxosRequestBatcher.class);

    private final ConcurrentMap<LeadershipToken, Batch> queue;
    private final Function<LeadershipToken, StillLeadingStatus> operation;

    public PaxosRequestBatcher(ExecutorService executor, Function<LeadershipToken, StillLeadingStatus> operation) {
        this.queue = Maps.newConcurrentMap();
        this.operation = operation;
        executor.execute(() -> sendRequests());
    }

    private void sendRequests() {
        while (true) {
            if (queue.isEmpty()) {
                continue;
            }

            for (Map.Entry<LeadershipToken, Batch> e : queue.entrySet()) {
                populateBatch(e.getValue(), e.getKey());
            }
        }
    }

    public StillLeadingStatus await(LeadershipToken key) throws InterruptedException, ExecutionException {
        Batch batch = getBatch(key);
        batch.populationLatch.await();
        if (batch.error != null) {
            throw new ExecutionException(batch.error);
        }
        return batch.ret;
    }

    private void populateBatch(Batch batch, LeadershipToken key) {
        try {
            /* Close only the current batch. If someone else has replaced this batch
             * already, don't close them prematurely; let them close themselves.
             */
            queue.remove(key, batch);
            StillLeadingStatus ret = operation.apply(key);
            batch.populate(ret);
        } catch (Throwable t) {
            log.error("Something went wrong while populating our batch", t);
            batch.error = t;
        } finally {
            batch.populationLatch.countDown();
        }
    }

    private Batch getBatch(LeadershipToken key) {
        Batch batch = queue.get(key);
        while (batch == null) {
            queue.putIfAbsent(key, new Batch());
            batch = queue.get(key);
        }
        return batch;
    }

    private static class Batch {
        public final CountDownLatch populationLatch;
        // should be written by 1 thread (the batching thread) and read by the rest
        public volatile StillLeadingStatus ret;
        public volatile Throwable error = null;

        public Batch() {
            // Counted down once by the thread that owns and created this call, after population
            this.populationLatch = new CountDownLatch(1);
        }

        public void populate(StillLeadingStatus ret) {
            this.ret = ret;
        }
    }
}
