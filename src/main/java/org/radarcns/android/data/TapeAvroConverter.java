/*
 * Copyright 2017 The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.android.data;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificRecord;
import org.radarcns.data.Record;
import org.radarcns.topic.AvroTopic;
import org.radarcns.util.BackedObjectQueue;
import org.radarcns.util.QueueFile;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Converts records from an AvroTopic for Tape
 */
public class TapeAvroConverter<K extends SpecificRecord, V extends SpecificRecord>
        implements BackedObjectQueue.Converter<Record<K, V>> {
    private final EncoderFactory encoderFactory;
    private final DecoderFactory decoderFactory;
    private final DatumWriter keyWriter;
    private final DatumWriter valueWriter;
    private final DatumReader keyReader;
    private final DatumReader valueReader;
    private final byte[] headerBuffer = new byte[8];
    private final GenericData specificData;
    private final Schema keySchema;
    private final Schema valueSchema;
    private final String topicName;
    private BinaryEncoder encoder;
    private BinaryDecoder decoder;

    public TapeAvroConverter(AvroTopic<K, V> topic, GenericData specificData) {
        encoderFactory = EncoderFactory.get();
        decoderFactory = DecoderFactory.get();
        keySchema = topic.getKeySchema();
        valueSchema = topic.getValueSchema();
        keyWriter = specificData.createDatumWriter(keySchema);
        valueWriter = specificData.createDatumWriter(valueSchema);
        keyReader = specificData.createDatumReader(keySchema);
        valueReader = specificData.createDatumReader(valueSchema);
        this.specificData = specificData;
        topicName = topic.getName();
        encoder = null;
        decoder = null;
    }

    @SuppressWarnings("unchecked")
    public Record<K, V> deserialize(InputStream in) throws IOException {
        int numRead = 0;
        do {
            numRead += in.read(headerBuffer, numRead, 8 - numRead);
        } while (numRead < 8);
        decoder = decoderFactory.binaryDecoder(in, decoder);

        long kafkaOffset = QueueFile.bytesToLong(headerBuffer, 0);
        try {
            K key = (K) keyReader.read(null, decoder);
            V value = (V) valueReader.read(null, decoder);

            if (!specificData.validate(keySchema, key)
                    || !specificData.validate(valueSchema, value)) {
                throw new IllegalArgumentException("Failed to validate given record in topic "
                        + topicName + "\n\tkey: " + key + "\n\tvalue: " + value);
            }

            return new Record<>(kafkaOffset, key, value);
        } catch (RuntimeException ex) {
            throw new IOException("Failed to deserialize object", ex);
        }
    }

    @SuppressWarnings("unchecked")
    public void serialize(Record<K, V> o, OutputStream out) throws IOException {
        QueueFile.longToBytes(o.offset, headerBuffer, 0);
        out.write(headerBuffer, 0, 8);
        encoder = encoderFactory.binaryEncoder(out, encoder);
        keyWriter.write(o.key, encoder);
        valueWriter.write(o.value, encoder);
        encoder.flush();
    }
}
