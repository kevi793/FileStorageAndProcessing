package com.kevi793.EventStorageAndProcessing.store;

import com.kevi793.EventStorageAndProcessing.Constant;
import com.kevi793.EventStorageAndProcessing.cache.Cache;
import com.kevi793.EventStorageAndProcessing.cache.FIFOCache;
import com.kevi793.EventStorageAndProcessing.processor.EventProcessor;
import com.kevi793.EventStorageAndProcessing.store.segment.Segment;
import com.kevi793.EventStorageAndProcessing.store.segment.SegmentName;
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
public class EventStore<T> {
    public static final long DEFAULT_MAX_SEGMENT_LOG_FILE_SIZE_IN_BYTES = 1024L;
    private static final String SEGMENT = "segment";

    private String name;
    private String dataDirPath;
    private Long maxEventLogSegmentFileSizeInBytes;
    private Cache<SegmentName, Segment> segmentCache;
    private Class<T> clazz;
    private Consumer<T> consumer;
    private int segmentCacheSize;

    private Segment currentSegment;
    private Path logDirPath;
    private EventProcessor<T> eventProcessor;

    private EventStore() {
    }

    public synchronized void write(Object payload) throws IOException {

        if (this.currentSegment.getEventLogSegmentFileSize() >= this.maxEventLogSegmentFileSizeInBytes) {
            this.currentSegment = this.getOrCreateAndGetSegmentFromCache(new SegmentName(this.currentSegment.getEventOffset() + this.currentSegment.getSegmentName().getNumberOfEventsBefore()));
        }

        this.currentSegment.write(payload);
    }

    public T read(long eventNumber) throws IOException {
        log.debug("Trying to read eventNumber {} under {}.", eventNumber, this.name);
        Segment segment = this.getSegment(eventNumber);
        if (segment == null) {
            log.error("No file segment present for the eventNumber: {} under {}.", eventNumber, this.name);
            return null;
        }

        if (segment.getEventOffset() == 0) {
            log.debug("File Segment for eventNumber {} under {} is empty.", eventNumber, this.name);
            return null;
        }

        int eventOffsetWithinSegment = (int) (eventNumber - segment.getSegmentName().getNumberOfEventsBefore());
        String serializedEvent = segment.read(eventOffsetWithinSegment);
        Event event = Event.from(serializedEvent);
        return event.getPayload(this.clazz);
    }

    private Segment getSegment(long targetEventOffset) throws IOException {
        SegmentName[] segmentNameEntities = Files.list(this.logDirPath).map(path -> path.getFileName().toString())
                .filter(fileName -> fileName.startsWith(SEGMENT))
                .map(this::getSegmentFileNameWithoutExtension)
                .distinct()
                .map(SegmentName::from)
                .toArray(SegmentName[]::new);

        Arrays.sort(segmentNameEntities, Comparator.comparingLong(SegmentName::getNumberOfEventsBefore));

        int low = 0;
        int high = segmentNameEntities.length - 1;

        while (low <= high) {
            int mid = low + (high - low) / 2;

            if (segmentNameEntities[mid].getNumberOfEventsBefore() == targetEventOffset) {
                return this.getOrCreateAndGetSegmentFromCache(segmentNameEntities[mid]);
            } else if (segmentNameEntities[mid].getNumberOfEventsBefore() > targetEventOffset) {
                if (mid - 1 >= 0 && segmentNameEntities[mid - 1].getNumberOfEventsBefore() <= targetEventOffset) {
                    return this.getOrCreateAndGetSegmentFromCache(segmentNameEntities[mid - 1]);
                } else {
                    high = mid - 1;
                }
            } else {
                if (mid + 1 <= high && segmentNameEntities[mid + 1].getNumberOfEventsBefore() > targetEventOffset) {
                    return this.getOrCreateAndGetSegmentFromCache(segmentNameEntities[mid]);
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
        this.segmentCache = new FIFOCache<>(this.segmentCacheSize);
        this.currentSegment = this.getLatestOrCreateSegmentIfNotExists();
        this.eventProcessor = new EventProcessor<>(this.consumer, this.logDirPath, this);
        this.eventProcessor.setDaemon(true);
        this.eventProcessor.start();
    }

    private Segment getLatestOrCreateSegmentIfNotExists() throws IOException {

        String[] segments = Files.list(this.logDirPath).map(path -> path.getFileName().toString())
                .filter(fileName -> fileName.startsWith(SEGMENT))
                .map(this::getSegmentFileNameWithoutExtension)
                .toArray(String[]::new);

        if (segments.length == 0) {
            return this.getOrCreateAndGetSegmentFromCache(new SegmentName(0));
        } else {
            long maxOffsetSoFar = Long.MIN_VALUE;
            SegmentName segmentNameOfInterest = null;

            for (String segment : segments) {
                SegmentName segmentName = SegmentName.from(segment);

                if (segmentName.getNumberOfEventsBefore() > maxOffsetSoFar) {
                    maxOffsetSoFar = segmentName.getNumberOfEventsBefore();
                    segmentNameOfInterest = segmentName;
                }
            }

            return this.getOrCreateAndGetSegmentFromCache(segmentNameOfInterest);
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

    private Segment getOrCreateAndGetSegmentFromCache(SegmentName segmentName) throws IOException {
        if (this.segmentCache.get(segmentName) == null) {
            this.segmentCache.put(segmentName, new Segment(this.logDirPath.toString(), segmentName));
        }

        return this.segmentCache.get(segmentName);
    }

    public static class EventStoreBuilder<T> {

        public static final int DEFAULT_SEGMENT_CACHE_SIZE = 10;
        private final EventStore<T> eventStore;

        public EventStoreBuilder(String dataDirPath, String name, Consumer<T> consumer, Class<T> clazz) {
            this.eventStore = new EventStore<>();
            this.eventStore.dataDirPath = dataDirPath;
            this.eventStore.name = name;
            this.eventStore.consumer = consumer;
            this.eventStore.clazz = clazz;
            this.eventStore.segmentCacheSize = DEFAULT_SEGMENT_CACHE_SIZE;
        }

        public EventStoreBuilder<T> maxSegmentLogFileSizeInBytes(long maxSegmentLogFileSize) {
            this.eventStore.maxEventLogSegmentFileSizeInBytes = maxSegmentLogFileSize;
            return this;
        }

        public EventStoreBuilder<T> fileSegmentCacheSize(int cacheSize) {
            this.eventStore.segmentCacheSize = cacheSize;
            return this;
        }

        public EventStore<T> build() throws IOException {
            this.eventStore.init();
            return this.eventStore;
        }
    }
}
