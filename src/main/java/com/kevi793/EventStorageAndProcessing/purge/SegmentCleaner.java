package com.kevi793.EventStorageAndProcessing.purge;

import com.kevi793.EventStorageAndProcessing.store.EventStore;
import com.kevi793.EventStorageAndProcessing.store.ProcessedEventsTracker;
import com.kevi793.EventStorageAndProcessing.store.segment.Segment;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

@Slf4j
public class SegmentCleaner extends Thread {

    private final ProcessedEventsTracker processedEventsTracker;
    private final EventStore eventStore;
    private final long cleanupInterval;

    public SegmentCleaner(ProcessedEventsTracker processedEventsTracker, EventStore eventStore, long cleanupInterval) {
        this.processedEventsTracker = processedEventsTracker;
        this.eventStore = eventStore;
        this.cleanupInterval = cleanupInterval;
    }

    @Override
    public void run() {
        while (true) {

            if (Thread.currentThread().isInterrupted()) {
                log.debug("Segment Cleaner thread is interrupted. Shutting down!");
                break;
            }

            log.debug("Trying to find any segment to be deleted.");
            try {
                Segment[] segments = this.eventStore.getAllSegments();
                Arrays.sort(segments, Comparator.comparingLong(s -> s.getSegmentName().getNumberOfEventsBefore()));
                long eventsProcessedSoFar = this.processedEventsTracker.getNumberOfEventsProcessedSoFar();

                for (int i = 1; i < segments.length; i++) {
                    if (segments[i].getSegmentName().getNumberOfEventsBefore() < eventsProcessedSoFar) {
                        segments[i - 1].delete();
                    }
                }

            } catch (IOException e) {
                log.error("Failed to get segments. Will retry after some time");
                e.printStackTrace();
            } finally {
                try {
                    log.debug("Going to sleep for {}.", this.cleanupInterval);
                    Thread.sleep(this.cleanupInterval);
                } catch (InterruptedException e) {
                    log.info("Segment cleaner thread is interrupted.");
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
