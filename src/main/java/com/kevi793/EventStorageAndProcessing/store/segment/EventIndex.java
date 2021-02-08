package com.kevi793.EventStorageAndProcessing.store.segment;

import lombok.Getter;

@Getter
public class EventIndex {

    private static final String DELIMITER = " ";
    private final int eventOffset;
    private final long startPosition;
    private final int size;

    public EventIndex(int eventOffset, long startPosition, int size) {
        this.eventOffset = eventOffset;
        this.startPosition = startPosition;
        this.size = size;
    }

    public static EventIndex from(String serializedEventIndex) {
        String[] parts = serializedEventIndex.split(DELIMITER);
        return new EventIndex(Integer.parseInt(parts[0]), Long.parseLong(parts[1]), Integer.parseInt(parts[2]));
    }

    @Override
    public String toString() {
        return String.format("%d%s%d%s%d", eventOffset, DELIMITER, startPosition, DELIMITER, size);
    }
}
