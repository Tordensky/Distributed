package hadoop_test;
import java.util.*;
import java.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordCount {
	
	public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, 
	IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		
		private Text word = new Text();
		
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter 
				reporter) throws IOException {
			
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				
				output.collect(word, one);
			}
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> 
	{
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, 
				Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			
			
			
			output.collect(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("wordcount");
		
		conf.setOutputKeyClass(Text.class);
		
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(MapClass.class);
		
		conf.setCombinerClass(Reduce.class);
		
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[1]));
		// conf.setInputPath(new Path(args[1]));
		
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));
		// conf.setOutputPath(new Path(args[2]));
		
		JobClient.runJob(conf);
	}
}