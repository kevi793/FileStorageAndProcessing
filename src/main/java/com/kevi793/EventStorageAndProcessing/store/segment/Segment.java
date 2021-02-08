package com.kevi793.EventStorageAndProcessing.store.segment;

import com.kevi793.EventStorageAndProcessing.Constant;
import com.kevi793.EventStorageAndProcessing.store.Event;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
@Getter
public class Segment {

    private static final String LOG_FILE_EXTENSION = "log";
    private static final String INDEX_FILE_EXTENSION = "index";

    private final String directory;
    private final SegmentName segmentName;

    private EventIndexSegment eventIndexSegment;
    private EventLogSegment eventLogSegment;

    private int eventOffset = 0;

    public Segment(String directory, SegmentName segmentName) throws IOException {
        this.directory = directory;
        this.segmentName = segmentName;
        this.init();
    }

    private void init() throws IOException {
        this.createDirectoryIfDoesNotExist();
        this.eventLogSegment = new EventLogSegment(Paths.get(directory, this.segmentName.toString() + Constant.DOT + LOG_FILE_EXTENSION));
        this.eventIndexSegment = new EventIndexSegment(Paths.get(directory, this.segmentName.toString() + Constant.DOT + INDEX_FILE_EXTENSION));
        EventIndex latestEventIndex = this.eventIndexSegment.getLatestEventIndex();
        if (latestEventIndex != null) {
            this.eventOffset = latestEventIndex.getEventOffset();
        }
    }

    public void write(Object payload) throws IOException {
        long currentLogSegmentFileSize = this.eventLogSegment.getFileSize();
        String messageString = new Event(this.eventOffset, payload).toString();
        this.eventLogSegment.write(messageString);

        EventIndex eventIndex = new EventIndex(this.eventOffset, currentLogSegmentFileSize, messageString.length());
        this.eventIndexSegment.write(eventIndex.toString());

        this.eventOffset++;
    }

    public String read(int offset) throws IOException {
        EventIndex eventIndex = this.eventIndexSegment.read(offset);

        if (eventIndex == null) {
            return null;
        }

        byte[] bytes = this.eventLogSegment.read(eventIndex.getStartPosition(), eventIndex.getSize());
        return new String(bytes);
    }

    public long getEventLogSegmentFileSize() throws IOException {
        return this.eventLogSegment.getFileSize();
    }

    private void createDirectoryIfDoesNotExist() throws IOException {
        Path directoryPath = Paths.get(this.directory);
        try {
            log.debug("Trying to create the directory if not exists: {}", directory);
            Files.createDirectory(directoryPath);
            log.debug("Directory created {}", this.directory);
        } catch (FileAlreadyExistsException e) {
            log.debug("Directory {} already exists!!", this.directory);
        } catch (IOException ex) {
            log.error("Could not create directory {}. Exception is {}", directory, ex);
            throw ex;
        }
    }
}
