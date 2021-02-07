import lombok.Getter;

@Getter
public class MessageIndex {

    private static final String DELIMITER = " ";
    private final int messageOffset;
    private final long startPosition;
    private final int size;

    public MessageIndex(int messageOffset, long startPosition, int size) {
        this.messageOffset = messageOffset;
        this.startPosition = startPosition;
        this.size = size;
    }

    public static MessageIndex from(String serializedLogIndexEntity) {
        String[] parts = serializedLogIndexEntity.split(DELIMITER);
        return new MessageIndex(Integer.parseInt(parts[0]), Long.parseLong(parts[1]), Integer.parseInt(parts[2]));
    }

    @Override
    public String toString() {
        return String.format("%d%s%d%s%d", messageOffset, DELIMITER, startPosition, DELIMITER, size);
    }
}
