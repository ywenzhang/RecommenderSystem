import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.PriorityQueue;

/**
 * Represent GenerateTopMovies with their details-- . *
 *
 * @author Yiwen Zhang
 */
public class GenerateTopMovies {
  public static class  GenerateTopMoviesMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] info = value.toString().split(",");
      String movieId = info[1];
      String rating = info[2];
      if(!rating.equals("rating")){
        DoubleWritable ratingScore = new DoubleWritable(Double.parseDouble(rating));
        context.write(new Text(movieId), ratingScore);
      }
    }
  }
  public static class GenerateTopMoviesMapperReducer extends Reducer<Text, DoubleWritable, Text, Text> {
    private int capacity = 100;
    private PriorityQueue<MovieRatingPair> pq;
    public void setup(Context context){
      pq = new PriorityQueue<>(capacity,new MovieRatingPairComparator());
    }
    public void reduce(Text key, Iterable<DoubleWritable> value, Context context){
      Double total = 0.0;
      for(DoubleWritable v:value){
        total += v.get();
      }
      if(pq.size()<capacity){
        MovieRatingPair pair = new MovieRatingPair(key.toString(),total);
        pq.add(pair);
      } else{
        MovieRatingPair compare = pq.peek();
        if(compare.rating<total){
          MovieRatingPair pair = new MovieRatingPair(key.toString(),total);
          pq.poll();
          pq.add(pair);
        }
      }
    }
    public void cleanup(Context context) throws IOException, InterruptedException {
      for(MovieRatingPair pair:pq){
        context.write(new Text(pair.movieId),null);
      }
    }
  }
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();
    Job job3 = Job.getInstance(conf, "JOB_1");
    job3.setJarByClass(GenerateTopMovies.class);
    job3.setMapperClass(GenerateTopMoviesMapper.class);
    job3.setReducerClass(GenerateTopMoviesMapperReducer.class);
    job3.setMapOutputKeyClass(Text.class);
    job3.setMapOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job3, new Path(args[0]));
    FileOutputFormat.setOutputPath(job3, new Path(args[1]));
    System.exit(job3.waitForCompletion(true) ? 0 : 1);
  }
}
