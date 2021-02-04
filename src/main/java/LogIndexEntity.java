import lombok.Getter;

@Getter
public class LogIndexEntity {

    private static final String DELIMITER = " ";
    private final int messageOffset;
    private final long startPosition;
    private final int size;

    public LogIndexEntity(int messageOffset, long startPosition, int size) {
        this.messageOffset = messageOffset;
        this.startPosition = startPosition;
        this.size = size;
    }

    public static LogIndexEntity from(String serializedLogIndexEntity) {
        String[] parts = serializedLogIndexEntity.split(DELIMITER);
        return new LogIndexEntity(Integer.parseInt(parts[0]), Long.parseLong(parts[1]), Integer.parseInt(parts[2]));
    }

    @Override
    public String toString() {
        return String.format("%d%s%d%s%d", messageOffset, DELIMITER, startPosition, DELIMITER, size);
    }
}
