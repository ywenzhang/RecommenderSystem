/**
 * Represent GroupByUsers with their details-- . *
 *
 * @author Yiwen Zhang
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Represent GenerateTopMovies with their details-- . *
 *
 * @author Yiwen Zhang
 */
public class GroupByUsers{
  public static class  GenerateTopMoviesMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] info = value.toString().split(",");
      String userId = info[0];
      String movieId = info[1];
      String rating = info[2];
      if(!rating.equals("rating")){
        context.write(new Text(userId), new Text(movieId +","+rating));
      }
    }
  }
  public static class GenerateTopMoviesMapperReducer extends Reducer<Text, Text, Text, Text> {
    private int users = 1000;

    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
      if (users != 0) {
        Map<String,String> records = new HashMap<String,String>();
        Double total = 0.0;
        Integer count = 0;
        for(Text v:value){
          String[] info = v.toString().split(",");
          records.put(info[0],info[1]);
          total += Double.valueOf(info[1]);
          count += 1;
        }
        if(count>15) {
          for(String k:records.keySet()) {
            context.write(new Text(key.toString()+","+k + "," + records.get(k).toString()), null);
          }
          context.write(new Text(key.toString() + ",avg," + String.valueOf(total / count)), null);
          users -= 1;
        }
      }
    }
  }
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();
    Job job3 = Job.getInstance(conf, "JOB_1");
    job3.setJarByClass(GroupByUsers.class);
    job3.setMapperClass(GenerateTopMoviesMapper.class);
    job3.setReducerClass(GenerateTopMoviesMapperReducer.class);
    job3.setMapOutputKeyClass(Text.class);
    job3.setMapOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job3, new Path(args[0]));
    FileOutputFormat.setOutputPath(job3, new Path(args[1]));
    System.exit(job3.waitForCompletion(true) ? 0 : 1);
  }
}