import exception.MalformedSegmentNameException;
import lombok.Getter;

@Getter
public class SegmentNameEntity {

    private static final String DELIMITER = "-";
    private final long messageOffset;
    private final String namePrefix = "segment";

    public SegmentNameEntity(long messageOffset) {
        this.messageOffset = messageOffset;
    }

    public static SegmentNameEntity from(String serializedSegmentName) {
        String[] tokens = serializedSegmentName.split(DELIMITER);

        if (tokens.length != 2) {
            throw new MalformedSegmentNameException(String.format("Given segment name %s is not valid.", serializedSegmentName));
        }

        return new SegmentNameEntity(Long.parseLong(tokens[1]));
    }

    @Override
    public String toString() {
        return String.format("%s%s%d", this.namePrefix, DELIMITER, this.messageOffset);
    }
}
