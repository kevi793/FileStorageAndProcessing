package com.kevi793.EventStorageAndProcessing.processor;

import com.kevi793.EventStorageAndProcessing.store.EventStore;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;

@Slf4j
public class EventProcessor<T> extends Thread {

    private static final String FILE_NAME = "processed.log";
    private static final int WAIT_TIME_IN_MS = 10000;
    private final Consumer<T> consumer;
    private final Path filePath;
    private final EventStore<T> eventStore;
    private Long eventNumber = 0L;

    public EventProcessor(Consumer<T> consumer, Path logDirPath, EventStore<T> eventStore) throws IOException {
        this.consumer = consumer;
        this.filePath = Paths.get(logDirPath.toString(), FILE_NAME);
        this.eventStore = eventStore;
        this.init();
    }

    private void init() throws IOException {
        log.debug("Try to create file to store processed logs offset at path {}.", this.filePath);
        if (Files.exists(this.filePath)) {
            log.info("{} already exists.", this.filePath);
            byte[] content = Files.readAllBytes(this.filePath);
            if (content.length > 0) {
                this.eventNumber = Long.parseLong(new String(content));
            }
        } else {
            Files.createFile(this.filePath);
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                log.debug("Trying to read eventNumber {}", this.eventNumber);
                T payload = this.eventStore.read(eventNumber);

                if (payload == null) {
                    log.debug("eventNumber does not exist");
                    Thread.sleep(WAIT_TIME_IN_MS);
                } else {
                    log.debug("Calling consumer for eventNumber {}", eventNumber);
                    this.consumer.accept(payload);
                    eventNumber++;
                    log.debug("Updating eventNumber {} in {}.", eventNumber, this.filePath);
                    Files.write(this.filePath, eventNumber.toString().getBytes(), StandardOpenOption.TRUNCATE_EXISTING);
                    log.debug("Successfully updated eventNumber {} in {}.", eventNumber, this.filePath);
                }
            } catch (IOException | InterruptedException e) {
                log.error("Error occurred while processing eventNumber {}", eventNumber);
                log.error("Exception is: {}", e);
                try {
                    Thread.sleep(WAIT_TIME_IN_MS);
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                }
            }
        }
    }
}