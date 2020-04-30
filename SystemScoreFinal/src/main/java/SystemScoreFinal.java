/**
 * Represent SystemScoreFinal with their details-- . *
 *
 * @author Yiwen Zhang
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class SystemScoreFinal {
  public static class SystemScoreFinalMapper extends Mapper<Object, Text, CompositeGroupKey, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] info = value.toString().split(",");

      context.write(new CompositeGroupKey(info[0],info[1]), new Text(info[2]));
    }
  }

  public static class MyPartitioner extends Partitioner<CompositeGroupKey, Text> {

    @Override
    public int getPartition(CompositeGroupKey groupedKey, Text value,
                            int numReduceTasks) {
      return (groupedKey.movieId.hashCode()+groupedKey.user.hashCode()) % numReduceTasks;
    }
  }

  public static class SystemScoreFinalReducer extends Reducer<CompositeGroupKey, Text, CompositeGroupKey, Text> {
    public void reduce(CompositeGroupKey key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
      Double score = 0.0;
      for(Text v:value){
        score += Double.parseDouble(v.toString());
      }
      context.write(key, new Text(String.valueOf(score)));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job3 = Job.getInstance(conf, "JOB_3");
    job3.setJarByClass(SystemScoreFinal.class);
    job3.setInputFormatClass(TextInputFormat.class);
    job3.setMapperClass(SystemScoreFinalMapper.class);
    job3.setReducerClass(SystemScoreFinalReducer.class);
    job3.setNumReduceTasks(10);
    job3.setPartitionerClass(MyPartitioner.class);
    job3.setMapOutputKeyClass(CompositeGroupKey.class);
    job3.setCombinerClass(SystemScoreFinalReducer.class);
    job3.setMapOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job3, new Path(args[0]));
    FileOutputFormat.setOutputPath(job3, new Path(args[1]));
    System.exit(job3.waitForCompletion(true) ? 0 : 1);
  }
}
