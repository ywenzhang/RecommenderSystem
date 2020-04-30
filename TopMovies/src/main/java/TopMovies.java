import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

/**
 * Represent TopMovies with their details-- . *
 *
 * @author Yiwen Zhang
 */
public class TopMovies {
  public static class GroupByUserMapper extends Mapper<Object, Text, Text, NullWritable> {
    Set<String> top100;
    public void setup(Context context) {
      top100 = new HashSet<String>();
      try {
        File file = new File("top100movies.csv");    //creates a new file instance
        FileReader fr = new FileReader(file);   //reads the file
        BufferedReader br = new BufferedReader(fr);  //creates a buffering character input stream
        String line;
        while ((line = br.readLine()) != null) {
          top100.add(line);
        }
        fr.close();
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] parts = value.toString().split(":");
      String[] movieIds = parts[0].split(",");
      String movieId1 = movieIds[0];
      if(top100.contains(movieId1)){
        context.write(value, NullWritable.get());
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job1 = Job.getInstance(conf, "JOB_1");
    job1.setJarByClass(TopMovies.class);
    job1.setMapperClass(GroupByUserMapper.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(NullWritable.class);
    job1.setNumReduceTasks(10);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
    System.exit(job1.waitForCompletion(true) ? 0 : 1);
  }
}
