/**
 * Represent Co_occurrence with their details-- . *
 *
 * @author Yiwen Zhang
 */
import java.io.*;
import java.util.HashSet;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
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
public class Co_occurrence {

  public static class GroupByUserMapper extends Mapper<Object, Text, Text, Text> {
    final static Integer USER_COLUMN = 0;
    final static Integer MOVIE_ID_COLUMN = 1;
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      CSVParser parser = new CSVParserBuilder().withSeparator(',').build();
      String[] line = parser.parseLine(value.toString());
      if(!line[0].equals("userId")&&!line[1].equals("avg")) {
        context.write(new Text(line[USER_COLUMN]), new Text(line[MOVIE_ID_COLUMN]));
      }
    }
  }


  public static class GroupByUserReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      StringBuilder sb = new StringBuilder(key.toString());
      for(Text movieID:values){
        sb.append(",");
        sb.append(movieID.toString());
      }
      context.write(new Text(sb.toString()),null);
    }
  }

  public static class MatrixMapper extends Mapper<Object, Text, CompositeGroupKey,IntWritable> {
    HashSet<String> top100;
    public void setup(Context context){
      top100 = new HashSet<String>();
      top100.add("1208");
      top100.add("33794");
      top100.add("3996");
      top100.add("5445");
      top100.add("367");
      top100.add("4963");
      top100.add("1073");
      top100.add("1258");
      top100.add("3147");
      top100.add("1206");
      top100.add("1961");
      top100.add("924");
      top100.add("3793");
      top100.add("2997");
      top100.add("6874");
      top100.add("349");
      top100.add("111");
      top100.add("1682");
      top100.add("6377");
      top100.add("1221");
      top100.add("316");
      top100.add("4886");
      top100.add("733");
      top100.add("2329");
      top100.add("750");
      top100.add("912");
      top100.add("4995");
      top100.add("1193");
      top100.add("344");
      top100.add("34");
      top100.add("1527");
      top100.add("1617");
      top100.add("1265");
      top100.add("293");
      top100.add("260");
      top100.add("2716");
      top100.add("500");
      top100.add("4973");
      top100.add("7361");
      top100.add("377");
      top100.add("590");
      top100.add("1136");
      top100.add("1721");
      top100.add("3578");
      top100.add("1240");
      top100.add("1213");
      top100.add("1291");
      top100.add("648");
      top100.add("1580");
      top100.add("778");
      top100.add("457");
      top100.add("5952");
      top100.add("1200");
      top100.add("608");
      top100.add("79132");
      top100.add("7153");
      top100.add("858");
      top100.add("595");
      top100.add("50");
      top100.add("597");
      top100.add("2858");
      top100.add("380");
      top100.add("1089");
      top100.add("1");
      top100.add("1036");
      top100.add("2762");
      top100.add("592");
      top100.add("780");
      top100.add("165");
      top100.add("296");
      top100.add("356");
      top100.add("593");
      top100.add("6539");
      top100.add("589");
      top100.add("32");
      top100.add("364");
      top100.add("588");
      top100.add("4226");
      top100.add("2028");
      top100.add("2959");
      top100.add("1270");
      top100.add("47");
      top100.add("318");
      top100.add("150");
      top100.add("527");
      top100.add("4993");
      top100.add("1097");
      top100.add("2571");
      top100.add("1198");
      top100.add("480");
      top100.add("58559");
      top100.add("1214");
      top100.add("541");
      top100.add("4306");
      top100.add("1197");
      top100.add("1704");
      top100.add("110");
      top100.add("1196");
      top100.add("1210");
      top100.add("736");
    }
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      CSVParser parser = new CSVParserBuilder().withSeparator(',').build();
      String[] line = parser.parseLine(value.toString());
      for(int i=1; i<line.length; i++){
        for(int j=1; j<line.length; j++){
          if(top100.contains(line[i])) {
            context.write(new CompositeGroupKey(line[i], line[j]), new IntWritable(1));
          }
        }
      }
    }
  }

  public static class MyPartitioner extends Partitioner<CompositeGroupKey, IntWritable> {

    @Override
    public int getPartition(CompositeGroupKey groupedKey, IntWritable value,
                            int numReduceTasks) {
      String movieId1= groupedKey.movieId1;
      String movieId2= groupedKey.movieId2;

      return ((movieId1+movieId2).hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  public static class MyGroupComparator extends WritableComparator {

    protected MyGroupComparator() {
      super(CompositeGroupKey.class, true);
    }
    // Overwrite group comparator to group the values based only on airline Id
    @Override
    public int compare(WritableComparable a, WritableComparable b) {

      CompositeGroupKey a_key = (CompositeGroupKey) a;
      CompositeGroupKey b_key = (CompositeGroupKey) b;

      String thisKey = a_key.movieId1;
      String thatKey = b_key.movieId1;
      int res = thisKey.compareTo(thatKey);
      if (res==0){
        thisKey = a_key.movieId2;
        thatKey = b_key.movieId2;
        res = thisKey.compareTo(thatKey);
      }
      return res;
    }
  }

  public static class MatrixReducer extends Reducer<CompositeGroupKey, IntWritable, Text, IntWritable> {

    public void reduce(CompositeGroupKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      Integer count = 0;
      for(IntWritable v:values){
        count += v.get();
      }
      context.write(new Text(key.toString()), new IntWritable(count));
    }
  }

  public static class NormalizationMapper extends Mapper<Text, Text, CompositeGroupKey, Text> {

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
      String[] pair = key.toString().split(",");
      String movieId1 = pair[0];
      String movieId2 = pair[1];
      Integer count = Integer.parseInt(value.toString());
      context.write(new CompositeGroupKey(movieId1,movieId2),new Text(movieId2+"="+count));
      context.write(new CompositeGroupKey(movieId1,"0"),new Text("total"+"="+count));
    }
  }

  public static class NormalizerPartitioner extends Partitioner<CompositeGroupKey, Text> {

    @Override
    public int getPartition(CompositeGroupKey groupedKey, Text value,
                            int numReduceTasks) {
      String movieId1= groupedKey.movieId1;

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
      Integer total= 0;
      String movieId1 = key.movieId1;
      for(Text v:values){
        String[] info = v.toString().split("=");
        String movieId2 = info[0];
        Integer value = Integer.parseInt(info[1]);
        if(info[0].equals("total")){
          total += value;
        }else{
          Double prob = Double.valueOf(value)/Double.valueOf(total);
          context.write(new Text(movieId1 + "," + movieId2 +":" +prob),null);
        }
      }
    }
  }


  public static void main(String[] args) throws Exception {

    String inputDir = "s3://finalprojectcs6240/ratings.csv";
    String outputTemp1Dir = "s3://finalprojectcs6240/output/Temp1";
    String outputTemp2Dir = "s3://finalprojectcs6240/output/Temp2";
    String outputFinalDir = "s3://finalprojectcs6240/output/Final";
    Configuration conf = new Configuration();
    Job job1 = Job.getInstance(conf, "JOB_1");
    job1.setJarByClass(Co_occurrence.class);
    job1.setMapperClass(GroupByUserMapper.class);
    job1.setReducerClass(GroupByUserReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setNumReduceTasks(10);
    job1.setMapOutputValueClass(Text.class);
    job1.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job1, new Path(inputDir));
    FileOutputFormat.setOutputPath(job1, new Path(outputTemp1Dir));
    boolean success = job1.waitForCompletion(true);
    if (success) {
      Job job2 = Job.getInstance(conf, "JOB_2");
      job2.setJarByClass(Co_occurrence.class);
      job2.setMapperClass(MatrixMapper.class);
      job2.setReducerClass(MatrixReducer.class);
      job2.setGroupingComparatorClass(MyGroupComparator.class);
      job2.setPartitionerClass(MyPartitioner.class);
      job2.setNumReduceTasks(10);
      job2.setMapOutputKeyClass(CompositeGroupKey.class);
      job2.setMapOutputValueClass(IntWritable.class);
      FileInputFormat.addInputPath(job2, new Path(outputTemp1Dir));
      FileOutputFormat.setOutputPath(job2, new Path(outputTemp2Dir));
      success = job2.waitForCompletion(true);
    }
    if (success) {
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
}
