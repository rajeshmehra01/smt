package com.example.converter;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.avro.AvroData;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

public class FixedAvroConverter extends AvroConverter {
    private AvroData avroData;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        super.configure(configs, isKey);
        avroData = new AvroData(1000) {
            @Override
            public SchemaAndValue toConnectData(String topic, Object value) {
                if (value instanceof byte[] bytes) {
                    try {
                        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
                        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>();
                        GenericRecord record = reader.read(null, decoder);
                        org.apache.kafka.connect.data.Schema schema = toConnectSchema(record.getSchema()).schema();
                        Object connectValue = fromConnectData(schema, record);
                        return new SchemaAndValue(schema, connectValue);
                    } catch (IOException e) {
                        throw new DataException("Failed to decode Avro record", e);
                    } catch (NumberFormatException e) {
                        if (e.getMessage().contains("Zero length BigInteger")) {
                            // skip invalid value
                            return SchemaAndValue.NULL;
                        }
                        throw e;
                    }
                }
                return super.toConnectData(topic, value);
            }
        };
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return avroData.toConnectData(topic, value);
    }
}
