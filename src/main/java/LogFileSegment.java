import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.*;
import java.util.List;

@Slf4j
@Getter
public class LogFileSegment {

    private static final String LOG_FILE_EXTENSION = "log";
    private static final String INDEX_FILE_EXTENSION = "index";
    private static final String NEW_LINE_DELIMITER = "\n";
    private static final String DOT = ".";

    private final String directory;
    private final SegmentNameEntity segmentNameEntity;

    @Value("${segment.file.size}")
    private Long maxFileSizeInBytes;

    private Path segmentLogFilePath;
    private Path segmentIndexFilePath;
    private long messageOffset = 0;

    public LogFileSegment(String directory, SegmentNameEntity segmentNameEntity) throws IOException {
        this.directory = directory;
        this.segmentNameEntity = segmentNameEntity;
        this.init();
    }

    public boolean canAcceptLogMessage(int incomingMessageLength) throws IOException {
        return Files.size(this.segmentLogFilePath) + incomingMessageLength <= this.maxFileSizeInBytes;
    }

    public synchronized void append(Object payload) throws IOException {
        long currentSegmentLogFileSize = Files.size(this.segmentLogFilePath);
        String logMessageString = this.appendNewLineInTheEnd(new LogMessage(this.messageOffset, payload).toString());
        Files.write(this.segmentLogFilePath, logMessageString.getBytes(), StandardOpenOption.APPEND);

        LogIndexEntity logIndexEntity = new LogIndexEntity(this.messageOffset, currentSegmentLogFileSize, logMessageString.length());
        Files.write(this.segmentIndexFilePath, this.appendNewLineInTheEnd(logIndexEntity.toString()).getBytes(), StandardOpenOption.APPEND);

        this.messageOffset++;
    }

    public String read(int offset) throws IOException {
        List<String> indexFileLines = Files.readAllLines(this.segmentIndexFilePath);
        LogIndexEntity logIndexEntity = LogIndexEntity.from(indexFileLines.get(offset));

        File segmentLogFile = new File(this.segmentLogFilePath.toString());
        RandomAccessFile randomAccessFile = new RandomAccessFile(segmentLogFile, "r");
        randomAccessFile.seek(logIndexEntity.getStartPosition());

        byte[] bytes = new byte[logIndexEntity.getSize()];
        randomAccessFile.readFully(bytes, 0, logIndexEntity.getSize());
        return new String(bytes);
    }

    private void init() throws IOException {
        this.ensureDirectoryExists();
        this.segmentLogFilePath = this.createSegmentLogFileIfNotExists();
        this.segmentIndexFilePath = this.createSegmentIndexFileIfNotExists();
    }

    private String appendNewLineInTheEnd(String input) {
        return input + NEW_LINE_DELIMITER;
    }

    private void ensureDirectoryExists() throws IOException {
        Path directoryPath = Paths.get(directory);

        try {
            log.debug("Trying to create the directory if not exists: {}", directory);
            Files.createDirectory(directoryPath);
        } catch (FileAlreadyExistsException e) {
            log.debug("Directory already existed!!");
        } catch (IOException ex) {
            log.error("Could not create directory {}. Exception is {}", directory, ex);
            throw ex;
        }
    }

    private Path createSegmentLogFileIfNotExists() throws IOException {
        Path path = Paths.get(directory, this.segmentNameEntity.toString() + DOT + LOG_FILE_EXTENSION);
        try {
            log.debug("Creating segment log file at path: {}", path.toString());
            Path filePath = Files.createFile(path);
            log.debug("Segment log file created at path: {}", filePath.toString());
            return filePath;
        } catch (FileAlreadyExistsException e) {
            log.debug("Segment log file at path: {} already exists.", path.toString());
            return path;
        } catch (IOException ex) {
            log.error("Failed to create segment log file: {}", path.toString());
            throw ex;
        }
    }

    private Path createSegmentIndexFileIfNotExists() throws IOException {
        Path path = Paths.get(directory, this.segmentNameEntity.toString() + DOT + INDEX_FILE_EXTENSION);
        try {
            log.debug("Creating segment index file at path: {}", path.toString());
            Path filePath = Files.createFile(path);
            log.debug("Segment index file created at path: {}", filePath.toString());
            return filePath;
        } catch (FileAlreadyExistsException e) {
            log.debug("Segment index file at path: {} already exists.", path.toString());
            List<String> lines = Files.readAllLines(path);
            LogIndexEntity lastIndex = LogIndexEntity.from(lines.get(lines.size() - 1));
            this.messageOffset = lastIndex.getMessageOffset() + 1;
            return path;
        } catch (IOException ex) {
            log.error("Failed to create segment index file: {}", path.toString());
            throw ex;
        }
    }

}
