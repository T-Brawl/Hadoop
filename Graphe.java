import java.io.IOException;
import java.util.*;

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

public class Graphe {

  public static class Map
       extends Mapper<Object, Text, Text, Text>{

    private Text couple = new Text();
    private Text amis = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String csvLine = value.toString();
      String delims = "[ ]+"; 
      String[] tokens = csvLine.split(delims);

      String liste = "";
      for(int i = 1; i < tokens.length - 1 ; i++) {
        liste += tokens[i]+" ";
      }
      liste += tokens[tokens.length - 1];
      
      amis.set(liste);
      
      for(int i = 1; i <= 100 ; i++) {
        if(Integer.parseInt(tokens[0]) <= i) {
          couple.set(tokens[0]+" "+i);
        } else {
          couple.set(i+" "+tokens[0]);
        }
        context.write(couple,amis);
      }  

    }
  }

  public static class Reduce
       extends Reducer<Text,Text,Text,Text> {
    private Text valeur = new Text();
    private Text cle = new Text();
    private int amil;
    private int amir;

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      String delims = "[ ]+"; 
      String[] amis = key.toString().split(delims);

      amil = Integer.parseInt(amis[0]);
      amir = Integer.parseInt(amis[1]);

      cle.set("Couple "+amis[0]+"-"+amis[1]+" : ");

      List<Integer> liste = new ArrayList<Integer>();
      String laListe = "";
      String tmp = "";
      
      for (Text val : values) {
        tmp += val.toString()+" ";        
      }

      String[] enCommun = tmp.split(delims);

      for(int i = 0; i < enCommun.length ; i++) {
        int tiers = Integer.parseInt(enCommun[i]);
        if ((tiers != amil) && (tiers != amir)) {
          if(liste.contains(tiers)) {
            laListe += enCommun[i]+",";
          } else {
            liste.add(tiers);
          }
        }
      }
      valeur.set(laListe);
      context.write(cle,valeur);

    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Graphe social");
    job.setJarByClass(Graphe.class);
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