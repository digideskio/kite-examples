package com.cloudera;

import org.apache.avro.generic.GenericRecord;
import org.apache.crunch.MapFn;

public class ImportRatings extends MapFn<GenericRecord, GenericRecord> {
  @Override
  public GenericRecord map(GenericRecord input) {
    Long ts = (Long) input.get("timestamp");
    ts *= 1000; // convert seconds to milliseconds
    input.put("timestamp", ts);
    return input;
  }
}
