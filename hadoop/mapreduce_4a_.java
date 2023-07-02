package FourA;
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
public class Satish1 {
public static class Map extends MapReduceBase implements
Mapper<LongWritable, Text, Text, IntWritable>{
@Override
public void map(LongWritable key, Text value,
OutputCollector<Text, IntWritable> out, Reporter
arg3)
throws IOException {
int one = 1 ;
String s1 = value.toString();
String[] data= s1.split(",");
// Column starts at 0 th index column 3 is username
out.collect(new Text(data[2]),new IntWritable(one));
}
}
public static class Reduce extends MapReduceBase implements
Reducer<Text, IntWritable, Text, IntWritable>{
@Override
public void reduce(Text key, Iterator<IntWritable> value,
OutputCollector<Text, IntWritable> out,
Reporter arg3) throws IOException {
// TODO Auto-generated method stub
int sum = 0 ;
while(value.hasNext()) {
sum += value.next().get();
}
out.collect(key, new IntWritable(sum));
}
}
public static void main(String[] args) throws IOException {
// TODO Auto-generated method stub
JobConf conf = new JobConf(Satish1.class);
conf.setJobName("ABC");//final command :- hadoop jar jarname hdfsinputPath
HDFSoutputPath
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
