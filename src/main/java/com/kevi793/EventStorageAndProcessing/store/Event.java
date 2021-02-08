package com.kevi793.EventStorageAndProcessing.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.SneakyThrows;

import java.sql.Timestamp;
import java.util.Date;

@Getter
public class Event {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final String EVENT_OFFSET = "offset";
    private static final String PAYLOAD = "payload";
    private static final String CREATE_TIME = "createTime";
    private static final String PAYLOAD_SIZE = "payloadSize";
    private static final String KEY_VALUE_SEPARATOR = ":";
    private static final String PROPERTY_SEPARATOR = " ";

    private final int eventOffset;
    private final String payloadString;
    private final Timestamp createTime;

    public Event(int offset, Object payload) throws JsonProcessingException {
        this.eventOffset = offset;
        this.createTime = new Timestamp(new Date().getTime());
        this.payloadString = objectMapper.writeValueAsString(payload);
    }

    public Event(int offset, Timestamp createTime, String payloadString) {
        this.eventOffset = offset;
        this.createTime = createTime;
        this.payloadString = payloadString;
    }

    public static Event from(String serializedEvent) {
        int offsetStartIndex = serializedEvent.indexOf(KEY_VALUE_SEPARATOR) + 1;
        int offsetEndIndex = serializedEvent.indexOf(PROPERTY_SEPARATOR, offsetStartIndex);
        int offset = Integer.parseInt(serializedEvent.substring(offsetStartIndex, offsetEndIndex));

        int createTimeStartIndex = serializedEvent.indexOf(KEY_VALUE_SEPARATOR, offsetEndIndex) + 1;
        int createTimeEndIndex = serializedEvent.indexOf(PROPERTY_SEPARATOR, createTimeStartIndex);
        Timestamp createTime = new Timestamp(Long.parseLong(serializedEvent.substring(createTimeStartIndex, createTimeEndIndex)));

        int payloadStartIndex = serializedEvent.indexOf(KEY_VALUE_SEPARATOR, createTimeEndIndex) + 1;
        int payloadEndIndex = serializedEvent.length();
        String payload = serializedEvent.substring(payloadStartIndex, payloadEndIndex);

        return new Event(offset, createTime, payload);
    }

    public <T> T getPayload(Class<T> clazz) throws JsonProcessingException {
        return objectMapper.readValue(this.payloadString, clazz);
    }

    @SneakyThrows
    @Override
    public String toString() {
        return String.format("%s%s%d", EVENT_OFFSET, KEY_VALUE_SEPARATOR, this.eventOffset) +
                PROPERTY_SEPARATOR +
                String.format("%s%s%d", CREATE_TIME, KEY_VALUE_SEPARATOR, this.createTime.getTime()) +
                PROPERTY_SEPARATOR +
                String.format("%s%s%s", PAYLOAD, KEY_VALUE_SEPARATOR, this.payloadString);
    }
}
