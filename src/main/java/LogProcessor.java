import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;

@Slf4j
public class LogProcessor<T> extends Thread {

    private static final String fileName = "processed.log";
    private static final int WAIT_TIME_IN_MS = 10000;
    private final Consumer<T> consumer;
    private final Path filePath;
    private final LogFile<T> logFile;
    private Long messageNumber = 0L;

    public LogProcessor(Consumer<T> consumer, Path logDirPath, LogFile<T> logFile) throws IOException {
        this.consumer = consumer;
        this.filePath = Paths.get(logDirPath.toString(), fileName);
        this.logFile = logFile;
        this.init();
    }

    private void init() throws IOException {
        log.debug("Create file to store processed logs offset.");
        if (Files.exists(this.filePath)) {
            log.info("{} already exists.", this.filePath);
            byte[] content = Files.readAllBytes(this.filePath);
            if (content.length > 0) {
                this.messageNumber = Long.parseLong(new String(content));
            }
        } else {
            Files.createFile(this.filePath);
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                log.debug("Trying to read messageNumber {}", this.messageNumber);
                T payload = this.logFile.read(messageNumber);

                if (payload == null) {
                    log.debug("messageNumber does not exist");
                    Thread.sleep(WAIT_TIME_IN_MS);
                } else {
                    log.debug("Calling consumer for messageNumber {}", messageNumber);
                    this.consumer.accept(payload);
                    messageNumber++;
                    log.debug("Updating messageNumber {} in {}.", messageNumber, this.filePath);
                    Files.write(this.filePath, messageNumber.toString().getBytes(), StandardOpenOption.TRUNCATE_EXISTING);
                    log.debug("Successfully updated messageNumber {} in {}.", messageNumber, this.filePath);
                }
            } catch (IOException | InterruptedException e) {
                log.error("Error occurred while processing messageNumber {}", messageNumber);
                log.error("Exception is: {}", e);
                try {
                    Thread.sleep(WAIT_TIME_IN_MS);
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                }
            }
        }
    }
}