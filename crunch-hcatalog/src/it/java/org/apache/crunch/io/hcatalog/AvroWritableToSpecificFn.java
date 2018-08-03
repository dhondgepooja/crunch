package org.apache.crunch.io.hcatalog;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.MapFn;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;



public class AvroWritableToSpecificFn<T extends SpecificRecord> extends MapFn<AvroGenericRecordWritable, T> {

    private final Class<T> readerClazz;

    /**
     * Instantiates a new instance, and configures the conversion to use the
     * specified {@code readerClazz}.
     *
     * @param readerClazz
     *          the avro class for conversion
     * @throws IllegalArgumentException if {@code readerClazz} is null
     */
    public AvroWritableToSpecificFn(Class<T> readerClazz) {
        if (readerClazz == null)
            throw new IllegalArgumentException("readerClazz cannot be null");

        this.readerClazz = readerClazz;
    }


    @Override
    public T map(AvroGenericRecordWritable input) {
        GenericRecord genericRecord = input.getRecord();
        AvroSerDe avroSerDe = new AvroSerDe();
        try {
            return readerClazz.cast(avroSerDe.deserialize(input));
        } catch (SerDeException e) {
            e.printStackTrace();
        }
        return null;
    }
}
