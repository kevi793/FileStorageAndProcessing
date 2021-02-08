package com.kevi793.EventStorageAndProcessing.store.segment;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@Slf4j
public class EventIndexSegment extends BaseSegmentFile {

    public EventIndexSegment(Path eventIndexSegmentFilePath) throws IOException {
        super(eventIndexSegmentFilePath);
    }

    public EventIndex read(int offset) throws IOException {
        log.debug("Trying to get index of the event at offset {} from file {}.", offset, this.filePath);
        List<String> lines = Files.readAllLines(this.filePath);

        if (lines.size() == 0) {
            log.debug("Index file {} is empty.", this.filePath);
            return null;
        }

        if (offset >= lines.size()) {
            log.debug("Offset {} not present in {}.", offset, this.filePath);
            return null;
        }

        return EventIndex.from(lines.get(offset));
    }

    public EventIndex getLatestEventIndex() throws IOException {
        List<String> lines = Files.readAllLines(this.filePath);
        return lines.size() == 0 ? null : EventIndex.from(lines.get(lines.size() - 1));
    }

}
