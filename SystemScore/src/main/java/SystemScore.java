import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Represent SystemScore with their details-- . *
 *
 * @author Yiwen Zhang
 */
public class SystemScore {
  public static class CoOccurrenceMapper extends Mapper<Object, Text, CompositeGroupKey, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] pairAndProb = value.toString().split(":");
      String[] pair = pairAndProb[0].split(",");
      String movieId1 = pair[0];
      String movieId2 = pair[1];
      context.write(new CompositeGroupKey(movieId2,"2","0"), new Text("movie"+":"+movieId1 + "=" + pairAndProb[1]));
    }
  }

  public static class RatingMapper extends Mapper<Object, Text, CompositeGroupKey, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] pair = value.toString().split(",");
      String userId = pair[0];
      String rating = pair[2];
      if(pair[1].equals("avg")){
        for(int i=0;i<10;i++){
          context.write(new CompositeGroupKey("0","0",String.valueOf(i+1)),new Text(userId+"="+rating));
        }
      }else{
        context.write(new CompositeGroupKey(pair[1],"1","0"),new Text("user"+":"+userId+"="+rating));
      }
    }
  }

  public static class MyPartitioner extends Partitioner<CompositeGroupKey, Text> {

    @Override
    public int getPartition(CompositeGroupKey groupedKey, Text value,
                            int numReduceTasks) {
      if(!groupedKey.times.equals("0")){
        return Integer.parseInt(groupedKey.times)%numReduceTasks;
      }
      return groupedKey.movieId.hashCode() % numReduceTasks;
    }
  }

  public static class MyComparator extends WritableComparator {

    protected MyComparator() {
      super(CompositeGroupKey.class, true);
    }

    // Overwrite group comparator to group the values based only on airline Id
    @Override
    public int compare(WritableComparable a, WritableComparable b) {

      CompositeGroupKey a_key = (CompositeGroupKey) a;
      CompositeGroupKey b_key = (CompositeGroupKey) b;

      String thisKey = a_key.movieId;
      String thatKey = b_key.movieId;
      return thisKey.compareTo(thatKey);
    }
  }

  public static class ScoreReducer extends Reducer<CompositeGroupKey, Text, Text, Text> {

    private Map<String,Double> average;
    public void setup(Context context){
      average = new HashMap<String, Double>();
    }

    public void reduce(CompositeGroupKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      Map<String,Double> scores = new HashMap<String, Double>();
      Set<String> diff = null;
      if(key.movieId.equals("0")){
        for(Text value:values){
          String[] ratingArray = value.toString().split("=");
          String userId = ratingArray[0];
          Double avg = Double.parseDouble(ratingArray[1]);
          average.put(userId,avg);
        }
      }else{
        for(Text value:values){
          String[] category = value.toString().split(":");
          if(category[0].equals("user")){
            String[] userAndRating = category[1].split("=");
            String userId = userAndRating[0];
            String rating = userAndRating[1];
            scores.put(userId,Double.parseDouble(rating));
          }else{
            if(diff == null){
              diff = new HashSet<String>();
              for(String k:average.keySet()){
                if(!scores.containsKey(k)){
                  diff.add(k);
                }
              }
            }
            String[] movieAndProb = category[1].split("=");
            String movieId = movieAndProb[0];
            Double prob = Double.parseDouble(movieAndProb[1]);
            for(String k1:scores.keySet()){
              context.write(new Text(movieId+","+k1+","+scores.get(k1)*prob),null);
            }
            for(String k2:diff){
              context.write(new Text(movieId+","+k2+","+average.get(k2)*prob),null);
            }
          }
        }
      }
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job3 = Job.getInstance(conf, "JOB_1");
    job3.setJarByClass(SystemScore.class);
    job3.setInputFormatClass(KeyValueTextInputFormat.class);
    job3.setReducerClass(ScoreReducer.class);
    job3.setGroupingComparatorClass(MyComparator.class);
    job3.setPartitionerClass(MyPartitioner.class);
    job3.setNumReduceTasks(10);
    job3.setMapOutputKeyClass(CompositeGroupKey.class);
    job3.setMapOutputValueClass(Text.class);
    MultipleInputs.addInputPath(job3,new Path(args[0]), TextInputFormat.class, CoOccurrenceMapper.class);
    MultipleInputs.addInputPath(job3,new Path(args[1]), TextInputFormat.class, RatingMapper.class);
    FileOutputFormat.setOutputPath(job3, new Path(args[2]));
    System.exit(job3.waitForCompletion(true) ? 0 : 1);
  }
}
