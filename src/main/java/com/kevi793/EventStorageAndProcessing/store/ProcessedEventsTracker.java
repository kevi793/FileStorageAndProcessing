package com.kevi793.EventStorageAndProcessing.store;

import com.kevi793.EventStorageAndProcessing.store.segment.BaseSegmentFile;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

@Slf4j
public class ProcessedEventsTracker extends BaseSegmentFile {

    public ProcessedEventsTracker(Path processedEventsTrackerFilePath) throws IOException {
        super(processedEventsTrackerFilePath);
    }

    public void write(long numberOfEventsProcessedSoFar) throws IOException {
        log.debug("Trying to update {} with eventNumber {}.", this.filePath, numberOfEventsProcessedSoFar);
        Files.write(this.filePath, String.valueOf(numberOfEventsProcessedSoFar).getBytes(), StandardOpenOption.TRUNCATE_EXISTING);
        log.debug("Successfully updated {} with numberOfEventsProessedSoFar {}.", this.filePath, numberOfEventsProcessedSoFar);
    }

    public long getNumberOfEventsProcessedSoFar() throws IOException {
        byte[] bytes = Files.readAllBytes(this.filePath);
        if (bytes.length == 0) {
            return 0L;
        }

        return Long.parseLong(new String(bytes));
    }

}
