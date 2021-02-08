package com.kevi793.FileStorageAndProcessing.store.segment;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@Slf4j
public class SegmentIndexFile extends AbstractSegmentFile {

    public SegmentIndexFile(Path segmentIndexFilePath) throws IOException {
        super(segmentIndexFilePath);
    }

    public MessageIndex read(int offset) throws IOException {
        log.debug("Trying to get message index for offset {} from file {}.", offset, this.filePath);
        List<String> lines = Files.readAllLines(this.filePath);

        if (lines.size() == 0) {
            log.debug("Index file {} is empty.", this.filePath);
            return null;
        }

        if (offset >= lines.size()) {
            log.debug("Offset {} not present in {}.", offset, this.filePath);
            return null;
        }

        return MessageIndex.from(lines.get(offset));
    }

    public MessageIndex getLatestMessageIndex() throws IOException {
        List<String> lines = Files.readAllLines(this.filePath);
        return lines.size() == 0 ? null : MessageIndex.from(lines.get(lines.size() - 1));
    }

}
