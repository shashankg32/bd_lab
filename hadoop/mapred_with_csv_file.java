// Use the Hadoop framework to write a MapReduce program to read a .csv file into a single node
// Hadoop cluster containing following fields: Sl. No., CARD Name, Username, Amount withdrawn

// Implement the following:
// 1. Count the Number of transactions done by each user
// 2. Find the total amount of money transacted by each user


import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
public class mapred_with_csv_file
{

//map
public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>
{
private final static IntWritable one = new IntWritable(1);

@Override
public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException 
{
// TODO Auto-generated method stub
String valueString = value.toString();
String[] data = valueString.split(",");
output.collect(new Text(data[2]), one);
}
}

//reduce
public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> 
{
@Override
public void reduce(Text t_key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException 
{
// TODO Auto-generated method stub
Text key = t_key;
int frequency = 0;
while(values.hasNext()) 
{
IntWritable value=(IntWritable) values.next();
frequency += value.get();
}
output.collect(key, new IntWritable(frequency));
}
}

//main
public static void main(String[] args) throws IOException 
{ 
JobConf conf = new JobConf(lp4.class);
conf.setJobName("BankTransaction");

conf.setOutputKeyClass(Text.class);
conf.setOutputValueClass(IntWritable.class);

conf.setMapperClass(Map.class);
conf.setCombinerClass(Reduce.class);
conf.setReducerClass(Reduce.class);

conf.setInputFormat(TextInputFormat.class);
conf.setOutputFormat(TextOutputFormat.class);

FileInputFormat.setInputPaths(conf, new Path(args[0]));
FileOutputFormat.setOutputPath(conf, new Path(args[1]));

JobClient.runJob(conf);
}
}

