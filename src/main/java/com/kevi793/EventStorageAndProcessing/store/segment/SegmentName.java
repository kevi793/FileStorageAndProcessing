package com.kevi793.EventStorageAndProcessing.store.segment;

import com.kevi793.EventStorageAndProcessing.exception.MalformedSegmentNameException;
import lombok.Getter;

@Getter
public class SegmentName {

    private static final String DELIMITER = "-";
    private final long numberOfEventsBefore;
    private final String namePrefix = "segment";

    public SegmentName(long numberOfEventsBefore) {
        this.numberOfEventsBefore = numberOfEventsBefore;
    }

    public static SegmentName from(String serializedSegmentName) {
        String[] tokens = serializedSegmentName.split(DELIMITER);

        if (tokens.length != 2) {
            throw new MalformedSegmentNameException(String.format("Given segment name %s is not valid.", serializedSegmentName));
        }

        return new SegmentName(Long.parseLong(tokens[1]));
    }

    @Override
    public String toString() {
        return String.format("%s%s%d", this.namePrefix, DELIMITER, this.numberOfEventsBefore);
    }
}
