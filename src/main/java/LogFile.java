import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
@Getter
public class LogFile {

    private String name;

    private static final String SEGMENT = "segment";

    LogFileSegment currentLogFileSegment;

    @Value("${data.dir.path}")
    private String dataDirPath;

    public LogFile(String name) throws IOException {
        this.name = name;
        this.init();
    }

    private void init() throws IOException {

//        Stream<Path> paths = Files.list(logDirPath);
//
//        paths.anyMatch(path -> path.getFileName().startsWith(SEGMENT))
//



    }

    private LogFileSegment getLatestOrCreateSegmentIfNotExists() throws IOException {
        Path logDirPath = Paths.get(this.dataDirPath, this.name);
        Stream<Path> paths = Files.list(logDirPath);

        if (paths.count() == 0) {
            this.currentLogFileSegment = new LogFileSegment(logDirPath.toString(), SEGMENT, 0);
        }
        else {

        }

    }

    private void setupLogDirIfNotExists() throws IOException {
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
        }
        else {
            log.debug(String.format("LogDir already exists for %s", this.name));
        }
    }


}
