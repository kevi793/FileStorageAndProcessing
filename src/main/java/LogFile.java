import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;

@Slf4j
@Getter
public class LogFile {
    public static final long DEFAULT_MAX_SEGMENT_LOG_FILE_SIZE_IN_BYTES = 1024L;
    public static final String EMPTY_STRING = "";
    private static final String SEGMENT = "segment";
    private static final String DOT = ".";
    private final String name;
    private final String dataDirPath;
    private final Long maxSegmentLogFileSizeInBytes;

    private LogFileSegment currentLogFileSegment;
    private Path logDirPath;

    public LogFile(String dataDirPath, String name, Long maxSegmentLogFileSizeInBytes) throws IOException {
        this.dataDirPath = dataDirPath;
        this.name = name;
        this.maxSegmentLogFileSizeInBytes = maxSegmentLogFileSizeInBytes;
        this.init();
    }

    public LogFile(String dataDirPath, String name) throws IOException {
        this.dataDirPath = dataDirPath;
        this.name = name;
        this.maxSegmentLogFileSizeInBytes = DEFAULT_MAX_SEGMENT_LOG_FILE_SIZE_IN_BYTES;
        this.init();
    }

    public synchronized void write(Object payload) throws IOException {

        if (this.currentLogFileSegment.getSegmentLogFileSize() >= this.maxSegmentLogFileSizeInBytes) {
            this.currentLogFileSegment = new LogFileSegment(this.logDirPath.toString(), new SegmentNameEntity(this.currentLogFileSegment.getMessageOffset() + this.currentLogFileSegment.getSegmentNameEntity().getMessageOffset()));
        }

        this.currentLogFileSegment.append(payload);
    }

    public String read(long offset) throws IOException {

        LogFileSegment logFileSegment = this.getLogFileSegment(offset);
        if (logFileSegment == null) {
            log.error("Invalid message offset provided: {}", offset);
            return EMPTY_STRING;
        }

        return logFileSegment.read(offset - logFileSegment.getSegmentNameEntity().getMessageOffset());
    }

    private LogFileSegment getLogFileSegment(long targetMessageOffset) throws IOException {
        SegmentNameEntity[] segmentNameEntities = Files.list(this.logDirPath).map(path -> path.getFileName().toString())
                .filter(fileName -> fileName.startsWith(SEGMENT))
                .map(this::getSegmentFileNameWithoutExtension)
                .distinct()
                .map(SegmentNameEntity::from)
                .toArray(SegmentNameEntity[]::new);

        Arrays.sort(segmentNameEntities, Comparator.comparingLong(SegmentNameEntity::getMessageOffset));

        int low = 0;
        int high = segmentNameEntities.length - 1;

        while (low <= high) {
            int mid = low + (high - low) / 2;

            if (segmentNameEntities[mid].getMessageOffset() == targetMessageOffset) {
                return new LogFileSegment(this.logDirPath.toString(), segmentNameEntities[mid]);
            } else if (segmentNameEntities[mid].getMessageOffset() > targetMessageOffset) {
                if (mid - 1 >= 0 && segmentNameEntities[mid - 1].getMessageOffset() <= targetMessageOffset) {
                    return new LogFileSegment(this.logDirPath.toString(), segmentNameEntities[mid - 1]);
                } else {
                    high = mid - 1;
                }
            } else {
                if (mid + 1 <= high && segmentNameEntities[mid + 1].getMessageOffset() > targetMessageOffset) {
                    return new LogFileSegment(this.logDirPath.toString(), segmentNameEntities[mid]);
                } else {
                    low = mid + 1;
                }
            }
        }

        return null;
    }

    private String getSegmentFileNameWithoutExtension(String segmentFileNameWithExtension) {
        int indexOfDot = segmentFileNameWithExtension.indexOf(DOT);
        return segmentFileNameWithExtension.substring(0, indexOfDot);
    }

    private void init() throws IOException {
        this.logDirPath = this.getLogDirOrCreateIfNotExists();
        this.currentLogFileSegment = this.getLatestOrCreateSegmentIfNotExists();
    }

    private LogFileSegment getLatestOrCreateSegmentIfNotExists() throws IOException {
        Path logDirPath = Paths.get(this.dataDirPath, this.name);

        String[] segments = Files.list(this.logDirPath).map(path -> path.getFileName().toString())
                .filter(fileName -> fileName.startsWith(SEGMENT))
                .map(this::getSegmentFileNameWithoutExtension)
                .toArray(String[]::new);

        if (segments.length == 0) {
            return new LogFileSegment(logDirPath.toString(), new SegmentNameEntity(0));
        } else {
            long maxOffsetSoFar = Long.MIN_VALUE;
            SegmentNameEntity segmentNameEntityOfInterest = null;

            for (String segment : segments) {
                SegmentNameEntity segmentNameEntity = SegmentNameEntity.from(segment);

                if (segmentNameEntity.getMessageOffset() > maxOffsetSoFar) {
                    maxOffsetSoFar = segmentNameEntity.getMessageOffset();
                    segmentNameEntityOfInterest = segmentNameEntity;
                }
            }

            return new LogFileSegment(logDirPath.toString(), segmentNameEntityOfInterest);
        }
    }

    private Path getLogDirOrCreateIfNotExists() throws IOException {
        Path logDirPath = Paths.get(this.dataDirPath, this.name);
        log.debug(String.format("Checking if logDir already exists for %s", this.name));
        if (!Files.exists(logDirPath)) {
            log.debug(String.format("Trying to creating logDir for %s at path: %s", this.name, logDirPath.toString()));
            try {
                Files.createDirectory(logDirPath);
            } catch (IOException ex) {
                log.error(String.format("Failed to create logDir for %s at path: %s", this.name, logDirPath.toString()));
                throw ex;
            }
        } else {
            log.debug(String.format("LogDir already exists for %s", this.name));
        }

        return logDirPath;
    }
}
