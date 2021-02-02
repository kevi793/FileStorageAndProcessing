import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
@Getter
public class LogFile {
    private static final String SEGMENT = "segment";
    private static final String DOT = ".";

    private final String name;

    private final String dataDirPath;

    private LogFileSegment currentLogFileSegment;
    private Path logDirPath;

    public LogFile(String dataDirPath, String name) throws IOException {
        this.dataDirPath = dataDirPath;
        this.name = name;
        this.init();
    }

    public void write(Object payload) throws IOException {
        this.currentLogFileSegment.append(payload);
    }

    public String read(int offset) throws IOException {
        return this.currentLogFileSegment.read(offset);
    }

    private void init() throws IOException {
        this.logDirPath = this.getLogDirOrCreateIfNotExists();
        this.currentLogFileSegment = this.getLatestOrCreateSegmentIfNotExists();
    }

    private LogFileSegment getLatestOrCreateSegmentIfNotExists() throws IOException {
        Path logDirPath = Paths.get(this.dataDirPath, this.name);
        Path[] paths = Files.list(logDirPath).filter(path -> path.getFileName().startsWith(SEGMENT)).toArray(Path[]::new);

        if (paths.length == 0) {
            return new LogFileSegment(logDirPath.toString(), new SegmentNameEntity(0));
        } else {
            long maxOffsetSoFar = Long.MIN_VALUE;
            SegmentNameEntity segmentNameEntityOfInterest = null;

            for (Path path : paths) {
                String fileName = path.getFileName().toString();
                int indexOfDot = fileName.indexOf(DOT);
                SegmentNameEntity segmentNameEntity = SegmentNameEntity.from(fileName.substring(0, fileName.length() - 1 - indexOfDot));

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
            log.debug(String.format("Creating logDir for %s at path: %s", this.name, logDirPath.toString()));
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
