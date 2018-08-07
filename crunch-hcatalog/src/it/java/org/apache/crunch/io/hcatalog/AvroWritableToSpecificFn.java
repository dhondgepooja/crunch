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
import java.util.Properties;


public class AvroWritableToSpecificFn<T extends SpecificRecord> extends MapFn<AvroGenericRecordWritable, T> {

    @Override
    public T map(AvroGenericRecordWritable input) {
        return (T) new SpecificData().deepCopy(input.getFileSchema(), input.getRecord());
    }
}
