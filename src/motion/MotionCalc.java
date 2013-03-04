package motion;

import java.io.IOException;
import java.util.regex.*;
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

public class MotionCalc {

	public static class MapClass extends MapReduceBase implements Mapper<Text, Text, Text, Text>
	{	
		private Text outKey = new Text();
		private Text outValue = new Text();

		public void map(Text key, Text value, OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			
//			Gets filename
			String inputLine = key.toString();
			String[] parsed = inputLine.split("/");
			
			String filenameCurr = parsed[parsed.length - 1]; 
			String filenamePrev = parsed[parsed.length - 1];
			
			String camNum = parsed[parsed.length - 2];
			String frameNumStr = parseFrameNum(filenameCurr); 
			
			outKey.set(frameNumStr);
					
			outValue.set(camNum + "/" + filenameCurr);
			output.collect(outKey, outValue);	
			
			outValue.set(camNum + "/" + filenamePrev);
			output.collect(outKey, outValue);
		}
		
		private String parseFrameNum(String fileName){
			String re1=".*?";	// Non-greedy match on filler
		    String re2="(\\d+)";	// Integer Number 1		
			Pattern pattern = Pattern.compile(re1+re2,Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
			Matcher matcher = pattern.matcher(fileName); 
								
			String frameNum = null;
			
			if (matcher.find())
			{
				frameNum = matcher.group(1);
			}
			return frameNum;
		}
	}
	
	public static class ReduceClass extends MapReduceBase implements Reducer<Text, Text, Text, Text>
	{
		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			Text value = new Text();
			while(values.hasNext()){
				Text tmpValue = values.next();
				value.append(tmpValue.getBytes(), 0, tmpValue.getLength());
			}
			output.collect(key, value);
		}
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(MotionCalc.class);
		conf.setJobName("helloworld");
		
		conf.setOutputKeyClass(Text.class);
		
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(MapClass.class);
		
		conf.setCombinerClass(ReduceClass.class);
		
		conf.setReducerClass(ReduceClass.class);
		
		conf.setInputFormat(TestInpForm.class);
		
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]), new Path(args[1]));
		// conf.setInputPath(new Path(args[1]));
		
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));
		// conf.setOutputPath(new Path(args[2]));
		
		JobClient.runJob(conf);
	}

}
