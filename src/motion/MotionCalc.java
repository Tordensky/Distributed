package motion;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.regex.*;
import java.util.Iterator;

import javax.imageio.ImageIO;
import javax.imageio.stream.ImageInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobPriority;
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
			String filePath = key.toString();
			
			String[] parsed = filePath.split("/");
			
			String currFilename = parsed[parsed.length - 1];  		
			String prevFilename = generatePrevFileName(currFilename);
			
			String camNum = parsed[parsed.length - 2];
			
			String path = filePath.split("/(?!.*/.*)")[0]; 
					//getPathFromParsed(parsed);
			
			String frameNumStr = parseFrameNum(currFilename); 
			
			outKey.set(frameNumStr);
					
			double FrameValue = AutoProduce(path, currFilename, prevFilename);
			
			String resStr = camNum + "/" + FrameValue;
			outValue.set(resStr);
			
			output.collect(outKey, outValue);	
			
		}
		
		private String getPathFromParsed(String[] filePathList){
			String path = null;
			for (int i = 0; i < filePathList.length - 1; i++) {
				path += filePathList[i];
				path += "/";
			}
			return path;
		} 
		
		private double AutoProduce(String path, String currFilename, String prevFilename) throws IOException{
			
			Path pathCurr = new Path(path + "/" + currFilename);
			Path pathPrev = new Path(path + "/" + prevFilename);
			
			FileSystem fs = FileSystem.get(new Configuration());
					
			ImageInputStream currFrameFile = ImageIO.createImageInputStream(fs.open(pathCurr));
			ImageInputStream prevFrameFile = ImageIO.createImageInputStream(fs.open(pathPrev));
			
			BufferedImage prevFrame = ImageIO.read(currFrameFile); 
			BufferedImage currFrame = ImageIO.read(prevFrameFile);
			
			Color prevFrameColor, currFrameColor;
			
			int height = -1;
			int width = -1;
			
			if (height <0) { height = currFrame.getHeight(); }
			if (width <0) { width = currFrame.getWidth(); }
			
			int motionSum = 0;
			for (int y = 0; y < height; y++) {
				for (int x = 0; x < width; x++) {
					prevFrameColor = new Color(prevFrame.getRGB(x, y));
					currFrameColor = new Color(currFrame.getRGB(x, y));
					
					motionSum += Math.abs(prevFrameColor.getRed() - currFrameColor.getRed());
					motionSum += Math.abs(prevFrameColor.getRed() - currFrameColor.getRed());
					motionSum += Math.abs(prevFrameColor.getRed() - currFrameColor.getRed());
				}
			}
			double motionEstimate = motionSum / ((double) height*width);
			return motionEstimate;
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
		
		private String parseFrameNumFromPath(String filePath){
			String re1=".*?";	// Non-greedy match on filler
		    String re2="((?:[a-z][a-z\\.\\d\\-]+)\\.(?:[a-z][a-z\\-]+))(?![\\w\\.])";	// Integer Number 1		
			Pattern pattern = Pattern.compile(re1+re2,Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
			Matcher matcher = pattern.matcher(filePath); 
								
			String frameNum = null;
			
			if (matcher.find())
			{
				frameNum = matcher.group(1);
			}
			return frameNum;
		}
		
		private String generatePrevFileName(String fileName){
			int fileNumber = Integer.parseInt(parseFrameNum(fileName));
			if (fileNumber != 0){
				fileNumber -= 1;
			}
			String result = "frame" + String.format("%04d", fileNumber) + ".jpeg";
			return result;
		}
	}
	
	public static class ReduceClass extends MapReduceBase implements Reducer<Text, Text, Text, Text>
	{
		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
			double action = 0.0;
			
			String cam = "cam1";
			
			double nextAction = 0.0;
			
			while(values.hasNext()){					
				String res = values.next().toString();
				String[] splitRes = res.split("/");

				nextAction = Double.parseDouble(splitRes[1]);
				
					if (nextAction > action){
						action = nextAction;
						
						cam = splitRes[0];
					}	
			}
			output.collect(key, new Text(cam));
		}
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(MotionCalc.class);
		conf.setJobName("HappyCamperOnTheRocks");
		
		conf.setOutputKeyClass(Text.class);
		
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(MapClass.class);
		
		conf.setReducerClass(ReduceClass.class);
		
		conf.setInputFormat(TestInpForm.class);
		
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]), new Path(args[1]));
		// conf.setInputPath(new Path(args[1]));
		
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));
		// conf.setOutputPath(new Path(args[2]));
		
		//conf.setJobPriority(JobPriority.VERY_HIGH);
		JobClient.runJob(conf);
	}

}
