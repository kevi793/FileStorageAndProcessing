import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.SneakyThrows;

import java.sql.Timestamp;
import java.util.Date;

@Getter
public class LogMessage {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final String MESSAGE_OFFSET = "offset";
    private static final String PAYLOAD = "payload";
    private static final String CREATE_TIME = "createTime";
    private static final String PAYLOAD_SIZE = "payloadSize";
    private static final String KEY_VALUE_SEPARATOR = ":";
    private static final String PROPERTY_SEPARATOR = " ";

    private final int messageOffset;
    private final String payloadString;
    private final Timestamp createTime;

    public LogMessage(int offset, Object payload) throws JsonProcessingException {
        this.messageOffset = offset;
        this.createTime = new Timestamp(new Date().getTime());
        this.payloadString = objectMapper.writeValueAsString(payload);
    }

    public LogMessage(int offset, Timestamp createTime, String payloadString) {
        this.messageOffset = offset;
        this.createTime = createTime;
        this.payloadString = payloadString;
    }

    public static LogMessage from(String serializedLogMessage) throws JsonProcessingException {
        int offsetStartIndex = serializedLogMessage.indexOf(KEY_VALUE_SEPARATOR) + 1;
        int offsetEndIndex = serializedLogMessage.indexOf(PROPERTY_SEPARATOR, offsetStartIndex);
        int offset = Integer.parseInt(serializedLogMessage.substring(offsetStartIndex, offsetEndIndex));

        int createTimeStartIndex = serializedLogMessage.indexOf(KEY_VALUE_SEPARATOR, offsetEndIndex) + 1;
        int createTimeEndIndex = serializedLogMessage.indexOf(PROPERTY_SEPARATOR, createTimeStartIndex);
        Timestamp createTime = new Timestamp(Long.parseLong(serializedLogMessage.substring(createTimeStartIndex, createTimeEndIndex)));

        int payloadStartIndex = serializedLogMessage.indexOf(KEY_VALUE_SEPARATOR, createTimeEndIndex) + 1;
        int payloadEndIndex = serializedLogMessage.length() - 1;
        String payload = serializedLogMessage.substring(payloadStartIndex, payloadEndIndex);

        return new LogMessage(offset, createTime, payload);
    }

    public <T> T getPayload(Class<T> clazz) throws JsonProcessingException {
        return objectMapper.readValue(this.payloadString, clazz);
    }

    @SneakyThrows
    @Override
    public String toString() {
        return String.format("%s%s%d", MESSAGE_OFFSET, KEY_VALUE_SEPARATOR, this.messageOffset) +
                PROPERTY_SEPARATOR +
                String.format("%s%s%d", CREATE_TIME, KEY_VALUE_SEPARATOR, this.createTime.getTime()) +
                PROPERTY_SEPARATOR +
                String.format("%s%s%s", PAYLOAD, KEY_VALUE_SEPARATOR, this.payloadString);
    }
}
