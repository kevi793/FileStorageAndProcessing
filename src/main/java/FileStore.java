import cache.Cache;
import cache.FIFOCache;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.function.Consumer;

@Slf4j
@Getter
public class FileStore<T> {
    public static final long DEFAULT_MAX_SEGMENT_LOG_FILE_SIZE_IN_BYTES = 1024L;
    private static final String SEGMENT = "segment";

    private String name;
    private String dataDirPath;
    private Long maxSegmentLogFileSizeInBytes;
    private Cache<SegmentName, FileSegment> fileSegmentCache;
    private Class<T> clazz;
    private Consumer<T> consumer;
    private int fileSegmentCacheSize;

    private FileSegment currentFileSegment;
    private Path logDirPath;
    private LogProcessor<T> logProcessor;

    private FileStore() {
    }

    public synchronized void write(Object payload) throws IOException {

        if (this.currentFileSegment.getSegmentLogFileSize() >= this.maxSegmentLogFileSizeInBytes) {
            this.currentFileSegment = this.getOrCreateAndGetLogFileSegmentFromCache(new SegmentName(this.currentFileSegment.getMessageOffset() + this.currentFileSegment.getSegmentName().getNumberOfMessagesBefore()));
        }

        this.currentFileSegment.write(payload);
    }

    public T read(long messageNumber) throws IOException {
        log.debug("Trying to read messageNumber {} under {}.", messageNumber, this.name);
        FileSegment fileSegment = this.getFileSegment(messageNumber);
        if (fileSegment == null) {
            log.error("No file segment present for the messageNumber: {} under {}.", messageNumber, this.name);
            return null;
        }

        if (fileSegment.getMessageOffset() == 0) {
            log.debug("File Segment for messageNumber {} under {} is empty.", messageNumber, this.name);
            return null;
        }

        int messageOffsetWithinSegment = (int) (messageNumber - fileSegment.getSegmentName().getNumberOfMessagesBefore());
        String logMessageString = fileSegment.read(messageOffsetWithinSegment);
        Message message = Message.from(logMessageString);
        return message.getPayload(this.clazz);
    }

    private FileSegment getFileSegment(long targetMessageOffset) throws IOException {
        SegmentName[] segmentNameEntities = Files.list(this.logDirPath).map(path -> path.getFileName().toString())
                .filter(fileName -> fileName.startsWith(SEGMENT))
                .map(this::getSegmentFileNameWithoutExtension)
                .distinct()
                .map(SegmentName::from)
                .toArray(SegmentName[]::new);

        Arrays.sort(segmentNameEntities, Comparator.comparingLong(SegmentName::getNumberOfMessagesBefore));

        int low = 0;
        int high = segmentNameEntities.length - 1;

        while (low <= high) {
            int mid = low + (high - low) / 2;

            if (segmentNameEntities[mid].getNumberOfMessagesBefore() == targetMessageOffset) {
                return this.getOrCreateAndGetLogFileSegmentFromCache(segmentNameEntities[mid]);
            } else if (segmentNameEntities[mid].getNumberOfMessagesBefore() > targetMessageOffset) {
                if (mid - 1 >= 0 && segmentNameEntities[mid - 1].getNumberOfMessagesBefore() <= targetMessageOffset) {
                    return this.getOrCreateAndGetLogFileSegmentFromCache(segmentNameEntities[mid - 1]);
                } else {
                    high = mid - 1;
                }
            } else {
                if (mid + 1 <= high && segmentNameEntities[mid + 1].getNumberOfMessagesBefore() > targetMessageOffset) {
                    return this.getOrCreateAndGetLogFileSegmentFromCache(segmentNameEntities[mid]);
                } else {
                    low = mid + 1;
                }
            }
        }

        return null;
    }

    private String getSegmentFileNameWithoutExtension(String segmentFileNameWithExtension) {
        int indexOfDot = segmentFileNameWithExtension.indexOf(Constant.DOT);
        return segmentFileNameWithExtension.substring(0, indexOfDot);
    }

    private void init() throws IOException {
        this.logDirPath = this.getLogDirOrCreateIfNotExists();
        this.fileSegmentCache = new FIFOCache<>(this.fileSegmentCacheSize);
        this.currentFileSegment = this.getLatestOrCreateSegmentIfNotExists();
        this.logProcessor = new LogProcessor<>(this.consumer, this.logDirPath, this);
        this.logProcessor.setDaemon(true);
        this.logProcessor.start();
    }

    private FileSegment getLatestOrCreateSegmentIfNotExists() throws IOException {

        String[] segments = Files.list(this.logDirPath).map(path -> path.getFileName().toString())
                .filter(fileName -> fileName.startsWith(SEGMENT))
                .map(this::getSegmentFileNameWithoutExtension)
                .toArray(String[]::new);

        if (segments.length == 0) {
            return this.getOrCreateAndGetLogFileSegmentFromCache(new SegmentName(0));
        } else {
            long maxOffsetSoFar = Long.MIN_VALUE;
            SegmentName segmentNameOfInterest = null;

            for (String segment : segments) {
                SegmentName segmentName = SegmentName.from(segment);

                if (segmentName.getNumberOfMessagesBefore() > maxOffsetSoFar) {
                    maxOffsetSoFar = segmentName.getNumberOfMessagesBefore();
                    segmentNameOfInterest = segmentName;
                }
            }

            return this.getOrCreateAndGetLogFileSegmentFromCache(segmentNameOfInterest);
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

    private FileSegment getOrCreateAndGetLogFileSegmentFromCache(SegmentName segmentName) throws IOException {
        if (this.fileSegmentCache.get(segmentName) == null) {
            this.fileSegmentCache.put(segmentName, new FileSegment(this.logDirPath.toString(), segmentName));
        }

        return this.fileSegmentCache.get(segmentName);
    }

    public static class FileStoreBuilder<T> {

        public static final int DEFAULT_SEGMENT_CACHE_SIZE = 10;
        private final FileStore<T> fileStore;

        public FileStoreBuilder(String dataDirPath, String name, Consumer<T> consumer, Class<T> clazz) {
            this.fileStore = new FileStore<>();
            this.fileStore.dataDirPath = dataDirPath;
            this.fileStore.name = name;
            this.fileStore.consumer = consumer;
            this.fileStore.clazz = clazz;
            this.fileStore.fileSegmentCacheSize = DEFAULT_SEGMENT_CACHE_SIZE;
        }

        public FileStoreBuilder<T> maxSegmentLogFileSizeInBytes(long maxSegmentLogFileSize) {
            this.fileStore.maxSegmentLogFileSizeInBytes = maxSegmentLogFileSize;
            return this;
        }

        public FileStoreBuilder<T> fileSegmentCacheSize(int cacheSize) {
            this.fileStore.fileSegmentCacheSize = cacheSize;
            return this;
        }

        public FileStore<T> build() throws IOException {
            this.fileStore.init();
            return this.fileStore;
        }
    }
}
