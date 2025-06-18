
package com.example.kafka.converter;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.avro.AvroData;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Map;

public class SafeAvroConverter extends AvroConverter {

    private static final Logger log = LoggerFactory.getLogger(SafeAvroConverter.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        super.configure(configs, isKey);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        try {
            return super.toConnectData(topic, value);
        } catch (Exception e) {
            if (e.getCause() instanceof NumberFormatException && e.getCause().getMessage().contains("Zero length BigInteger")) {
                log.warn("Caught Zero length BigInteger on topic {}. Skipping record. Returning null value.", topic, e);
                return new SchemaAndValue(null, null);
            }
            throw e;
        }
    }
}
