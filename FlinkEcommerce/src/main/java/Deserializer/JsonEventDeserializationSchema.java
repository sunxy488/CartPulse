package Deserializer;

import DTO.EventEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class JsonEventDeserializationSchema implements DeserializationSchema<EventEnvelope> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public EventEnvelope deserialize(byte[] message) throws IOException {
        return mapper.readValue(message, EventEnvelope.class);
    }

    @Override
    public boolean isEndOfStream(EventEnvelope eventEnvelope) {
        return false;
    }

    @Override
    public TypeInformation<EventEnvelope> getProducedType() {
        return TypeInformation.of(EventEnvelope.class);
    }
}

