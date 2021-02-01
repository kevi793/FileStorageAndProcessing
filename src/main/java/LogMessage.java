import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.SneakyThrows;

import java.sql.Timestamp;
import java.util.Date;

@Getter
public class LogMessage {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final Long messageOffset;
    private final String payloadString;
    private final Timestamp createTime;
    private final Integer payloadSize;

    public LogMessage(long offset, Object payload) throws JsonProcessingException {
        this.messageOffset = offset;
        this.createTime = new Timestamp(new Date().getTime());
        this.payloadString = objectMapper.writeValueAsString(payload);
        this.payloadSize = this.payloadString.length();
    }

    public static LogMessage from(String serializedLogMessage) throws JsonProcessingException {
        return objectMapper.readValue(serializedLogMessage, LogMessage.class);
    }

    @SneakyThrows
    @Override
    public String toString() {
        return objectMapper.writeValueAsString(this);
    }
}
