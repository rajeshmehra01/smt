package com.example;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class SchemalessTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    @Override
    public R apply(R record) {
        Object value = record.value();

        // Handle null record values safely
        if (value == null) {
            return record; // Skip processing if value is null
        }

        if (!(value instanceof Map)) {
            throw new DataException("Expected Map for schemaless data, found: " + value.getClass());
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> valueMap = (Map<String, Object>) value;
        valueMap.put("processed_at", System.currentTimeMillis());

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                null, // value schema is null for schemaless
                valueMap,
                record.timestamp()
        );
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}
} 
