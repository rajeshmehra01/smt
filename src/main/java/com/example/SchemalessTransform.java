package com.example;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

@SuppressWarnings("rawtypes")
public class SchemalessTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    @Override
    public R apply(R record) {
        if (record.value() == null || record.valueSchema() != null) {
            return record; // Only transform schemaless records
        }

        if (!(record.value() instanceof Map)) {
            throw new DataException("Expected Map for schemaless data, found: " + record.value().getClass());
        }

        Map<String, Object> valueMap = (Map<String, Object>) record.value();

        // Example: add a timestamp field
        valueMap.put("processed_at", System.currentTimeMillis());

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                null, // schemaless
                valueMap,
                record.timestamp()
        );
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef(); // Add configs here if needed
    }

    @Override
    public void configure(Map<String, ?> configs) {}

    @Override
    public void close() {}
}
