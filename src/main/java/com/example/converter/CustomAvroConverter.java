package com.example.converter;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.connect.data.SchemaAndValue;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

public class CustomAvroConverter extends AvroConverter {

    private final AvroData avroData = new AvroData(1000);

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
                return new SchemaAndValue(null, null);
            }
            throw e;
        }
    }
}

