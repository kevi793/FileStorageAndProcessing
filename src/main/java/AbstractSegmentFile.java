import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

@Slf4j
public abstract class AbstractSegmentFile {

    protected final Path filePath;

    public AbstractSegmentFile(Path filePath) throws IOException {
        this.filePath = filePath;
        this.createSegmentFileIfNotExists();
    }

    public void append(String payload) throws IOException {
        Files.write(this.filePath, Util.appendNewLine(payload).getBytes(), StandardOpenOption.APPEND);
    }

    public long getFileSize() throws IOException {
        return Files.size(this.filePath);
    }

    private void createSegmentFileIfNotExists() throws IOException {
        log.debug("Trying to create segment file {}.", this.filePath);
        if (Files.exists(this.filePath)) {
            log.debug("The segment file {} already exists.", this.filePath);
            return;
        }

        try {
            Files.createFile(this.filePath);
            log.debug("Successfully created segment file {}", this.filePath);
        } catch (IOException ex) {
            log.error("Failed to create segment file {}.", this.filePath);
            throw ex;
        }
    }
}
