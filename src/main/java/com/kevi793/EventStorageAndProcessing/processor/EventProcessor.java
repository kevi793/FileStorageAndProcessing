package com.kevi793.EventStorageAndProcessing.processor;

import com.kevi793.EventStorageAndProcessing.store.EventStore;
import com.kevi793.EventStorageAndProcessing.store.ProcessedEventsTracker;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.function.Consumer;

@Slf4j
public class EventProcessor<T> extends Thread {
    private final Consumer<T> consumer;
    private final EventStore<T> eventStore;
    private final ProcessedEventsTracker processedEventsTracker;
    private final long waitTimeInMs;
    private Long eventNumber;

    public EventProcessor(ProcessedEventsTracker tracker, Consumer<T> consumer, EventStore<T> eventStore, long waitTimeInMs) throws IOException {
        this.processedEventsTracker = tracker;
        this.consumer = consumer;
        this.eventStore = eventStore;
        this.eventNumber = tracker.getNumberOfEventsProcessedSoFar();
        this.waitTimeInMs = waitTimeInMs;
    }

    @Override
    public void run() {
        while (true) {

            if (Thread.currentThread().isInterrupted()) {
                log.debug("Event processor thread is interrupted. Shutting down!");
                break;
            }

            try {
                log.debug("Trying to read eventNumber {}", this.eventNumber);
                T payload = this.eventStore.read(this.eventNumber);

                if (payload == null) {
                    log.debug("The eventNumber {} does not exist.", this.eventNumber);
                    Thread.sleep(this.waitTimeInMs);
                } else {
                    log.debug("Calling consumer for eventNumber {}.", eventNumber);
                    this.consumer.accept(payload);
                    eventNumber++;
                    this.processedEventsTracker.write(this.eventNumber);
                }
            } catch (IOException | InterruptedException e) {
                log.error("Error occurred while processing eventNumber {}. Exception is {}.", eventNumber, e);
                try {
                    Thread.sleep(this.waitTimeInMs);
                } catch (InterruptedException interruptedException) {
                    log.info("Event processor thread is interrupted.");
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}