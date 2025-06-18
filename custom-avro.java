package com.example.kafka.converter;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.avro.AvroData;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;

import java.util.Map;

public class SafeAvroConverter extends AvroConverter {

    private AvroData avroData;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        super.configure(configs, isKey);
        this.avroData = new AvroData(new AbstractConfig(SCHEMA_CONFIG, configs));
    }

    @Override
    public Object toConnectData(String topic, byte[] value) {
        try {
            return super.toConnectData(topic, value);
        } catch (DataException e) {
            if (e.getCause() instanceof java.lang.NumberFormatException &&
                e.getCause().getMessage().contains("Zero length BigInteger")) {
                // Log warning and skip this field or provide default
                System.err.println("Warning: Skipping zero-length BigInteger in topic " + topic);
                Schema nullSchema = SchemaBuilder.bytes().optional();
                return new org.apache.kafka.connect.data.SchemaAndValue(nullSchema, null);
            } else {
                throw e;
            }
        }
    }
}
