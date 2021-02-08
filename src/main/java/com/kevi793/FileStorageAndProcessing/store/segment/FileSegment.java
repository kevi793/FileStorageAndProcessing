package com.kevi793.FileStorageAndProcessing.store.segment;

import com.kevi793.FileStorageAndProcessing.Constant;
import com.kevi793.FileStorageAndProcessing.store.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
@Getter
public class FileSegment {

    private static final String LOG_FILE_EXTENSION = "log";
    private static final String INDEX_FILE_EXTENSION = "index";

    private final String directory;
    private final SegmentName segmentName;

    private SegmentIndexFile segmentIndexFile;
    private SegmentLogFile segmentLogFile;

    private int messageOffset = 0;

    public FileSegment(String directory, SegmentName segmentName) throws IOException {
        this.directory = directory;
        this.segmentName = segmentName;
        this.init();
    }

    private void init() throws IOException {
        this.createDirectoryIfDoesNotExist();
        this.segmentLogFile = new SegmentLogFile(Paths.get(directory, this.segmentName.toString() + Constant.DOT + LOG_FILE_EXTENSION));
        this.segmentIndexFile = new SegmentIndexFile(Paths.get(directory, this.segmentName.toString() + Constant.DOT + INDEX_FILE_EXTENSION));
        MessageIndex latestMessageIndex = this.segmentIndexFile.getLatestMessageIndex();
        if (latestMessageIndex != null) {
            this.messageOffset = latestMessageIndex.getMessageOffset();
        }
    }

    public void write(Object payload) throws IOException {
        long currentSegmentLogFileSize = this.segmentLogFile.getFileSize();
        String messageString = new Message(this.messageOffset, payload).toString();
        this.segmentLogFile.append(messageString);

        MessageIndex messageIndex = new MessageIndex(this.messageOffset, currentSegmentLogFileSize, messageString.length());
        this.segmentIndexFile.append(messageIndex.toString());

        this.messageOffset++;
    }

    public String read(int offset) throws IOException {
        MessageIndex messageIndex = this.segmentIndexFile.read(offset);

        if (messageIndex == null) {
            return null;
        }

        byte[] bytes = this.segmentLogFile.read(messageIndex.getStartPosition(), messageIndex.getSize());
        return new String(bytes);
    }

    public long getSegmentLogFileSize() throws IOException {
        return this.segmentLogFile.getFileSize();
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
