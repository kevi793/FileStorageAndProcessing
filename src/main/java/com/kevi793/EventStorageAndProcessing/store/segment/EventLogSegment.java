package com.kevi793.EventStorageAndProcessing.store.segment;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;

public class EventLogSegment extends BaseSegmentFile {

    private static final String READ_MODE = "r";

    public EventLogSegment(Path eventLogSegmentFilePath) throws IOException {
        super(eventLogSegmentFilePath);
    }

    public byte[] read(long offset, int size) throws IOException {
        File segmentLogFile = new File(this.filePath.toString());
        RandomAccessFile randomAccessFile = new RandomAccessFile(segmentLogFile, READ_MODE);
        randomAccessFile.seek(offset);

        byte[] bytes = new byte[size];
        randomAccessFile.readFully(bytes, 0, size);
        return bytes;
    }
}
