/**
 * Represent Co_occurrence with their details-- . *
 *
 * @author Yiwen Zhang
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Co_occurrence {
  public static class NormalizationMapper extends Mapper<Text, Text, CompositeGroupKey, Text> {

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
      String[] pair = key.toString().split(",");
      String movieId1 = pair[0];
      String movieId2 = pair[1];
      Integer count = Integer.parseInt(value.toString());
      context.write(new CompositeGroupKey(movieId1, movieId2), new Text(movieId2 + "=" + count));
      context.write(new CompositeGroupKey(movieId1, "0"), new Text("total" + "=" + count));
    }
  }

  public static class NormalizerPartitioner extends Partitioner<CompositeGroupKey, Text> {

    @Override
    public int getPartition(CompositeGroupKey groupedKey, Text value,
                            int numReduceTasks) {
      String movieId1 = groupedKey.movieId1;

      return (movieId1.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  public static class NormalizerComparator extends WritableComparator {

    protected NormalizerComparator() {
      super(CompositeGroupKey.class, true);
    }

    // Overwrite group comparator to group the values based only on airline Id
    @Override
    public int compare(WritableComparable a, WritableComparable b) {

      CompositeGroupKey a_key = (CompositeGroupKey) a;
      CompositeGroupKey b_key = (CompositeGroupKey) b;

      String thisKey = a_key.movieId1;
      String thatKey = b_key.movieId1;
      return thisKey.compareTo(thatKey);
    }
  }

  public static class NormalizationReducer extends Reducer<CompositeGroupKey, Text, Text, Text> {

    public void reduce(CompositeGroupKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      Integer total = 0;
      String movieId1 = key.movieId1;
      for (Text v : values) {
        String[] info = v.toString().split("=");
        String movieId2 = info[0];
        Integer value = Integer.parseInt(info[1]);
        if (info[0].equals("total")) {
          total += value;
        } else {
          if(value>10) {
            Double prob = Double.valueOf(value) / Double.valueOf(total);
            context.write(new Text(movieId1 + "," + movieId2 +":" +prob),null);
          }
        }
      }
    }
  }


  public static void main(String[] args) throws Exception {
    String outputTemp2Dir = "s3://finalprojectcs6240/output/Temp2";
    String outputFinalDir = "s3://finalprojectcs6240/output/FinalGreaterThan10";

    Configuration conf = new Configuration();
    Job job3 = Job.getInstance(conf, "JOB_3");
    job3.setJarByClass(Co_occurrence.class);
    job3.setInputFormatClass(KeyValueTextInputFormat.class);
    job3.setMapperClass(NormalizationMapper.class);
    job3.setReducerClass(NormalizationReducer.class);
    job3.setGroupingComparatorClass(NormalizerComparator.class);
    job3.setPartitionerClass(NormalizerPartitioner.class);
    job3.setNumReduceTasks(10);
    job3.setMapOutputKeyClass(CompositeGroupKey.class);
    job3.setMapOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job3, new Path(outputTemp2Dir));
    FileOutputFormat.setOutputPath(job3, new Path(outputFinalDir));
    System.exit(job3.waitForCompletion(true) ? 0 : 1);
  }
}
