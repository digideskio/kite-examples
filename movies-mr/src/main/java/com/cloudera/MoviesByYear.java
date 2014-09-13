package com.cloudera;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.mapreduce.DatasetKeyInputFormat;
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat;
import org.kitesdk.tools.TaskUtil;

public class MoviesByYear extends Configured implements Tool {

  private static final Pattern MOVIE_YEAR = Pattern.compile("[^(]*\\((\\d+)\\).*");
  private static final List<Schema.Field> fields = Lists.newArrayList(
      new Schema.Field("year", Schema.create(Schema.Type.STRING), null, null),
      new Schema.Field("count", Schema.create(Schema.Type.LONG), null, null));
  private static final Schema schema = Schema.createRecord(
      "YearCount", null, null, false);
  static {
    schema.setFields(fields);
  }

  public static class ExtractYear extends Mapper<GenericRecord, Void, Text, LongWritable> {
    private static final LongWritable ONE = new LongWritable(1L);
    private final Text year = new Text();

    @Override
    protected void map(GenericRecord movie, Void _, Context context)
        throws IOException, InterruptedException {
      Matcher matcher = MOVIE_YEAR.matcher(movie.get("title").toString());
      if (matcher.matches()) {
        year.set(matcher.group(1));
      } else {
        year.set("unknown");
      }
      context.write(year, ONE);
    }
  }

  public static class Sum extends Reducer<Text, LongWritable, GenericRecord, Void> {
    private final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
    @Override
    protected void reduce(Text year, Iterable<LongWritable> counts, Context context)
        throws IOException, InterruptedException {
      long total = 0L;
      for (LongWritable count : counts) {
        total += count.get();
      }

      builder.set("year", year.toString());
      builder.set("count", total);

      context.write(builder.build(), null);
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: <input-uri> <output-uri>");
      return 1;
    }

    String outputUri = args[1];
    if (!Datasets.exists(outputUri)) {
      Datasets.create(outputUri, new DatasetDescriptor.Builder()
          .schema(schema)
          .build());
    }

    Job job = new Job(getConf(), "Movies by year histogram");
    job.setJarByClass(MoviesByYear.class);
    job.setMapperClass(ExtractYear.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setReducerClass(Sum.class);
    job.setInputFormatClass(DatasetKeyInputFormat.class);
    job.setOutputFormatClass(DatasetKeyOutputFormat.class);

    DatasetKeyInputFormat.configure(job)
        .readFrom(args[0]);
    DatasetKeyOutputFormat.configure(job)
        .overwrite(outputUri);

    TaskUtil.configure(job)
        .addJarPathForClass(HiveConf.class);

    Configuration conf = job.getConfiguration();
    conf.setBoolean("mapred.map.child.log.level", true);
    conf.setBoolean("mapred.reduce.child.log.level", true);
//    conf.set("mapred.child.java.opts",
//        "-Dlog4j.configuration=debug-log4j.properties");

    job.waitForCompletion(true);

    return job.isSuccessful() ? 0 : 1;
  }

  public static void main(String... args) throws Exception {
    int rc = ToolRunner.run(new MoviesByYear(), args);
    System.exit(rc);
  }
}
