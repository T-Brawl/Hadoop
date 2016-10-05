import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Poste {

  public static class Map
       extends Mapper<Object, Text, Text, Text>{

    //private final static IntWritable one = new IntWritable(1);
    private Text city = new Text();
    private Text coord = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String csvLine = value.toString();
      String delims = "[;]"; 
      String[] tokens = csvLine.split(delims);

      city.set(tokens[8]);
      coord.set(tokens[10]);
      
      context.write(city,coord);

    }
  }

  public static class Reduce
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      String answer = "";
      for (Text val : values) {
        sum ++;
        answer += "("+val.toString()+") ";
      }
      result.set(" ("+sum+") : "+answer);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Poste");
    job.setJarByClass(Poste.class);
    job.setMapperClass(Map.class);
    //job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}