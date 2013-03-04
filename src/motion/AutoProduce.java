package motion;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import javax.imageio.ImageIO;

//compile: javac AutoProduce.java
//usage: java AutoProduce cam1dir cam2dir		
//
//Note that this is just a demo of the algorithm to be used with Part 2 of Mandatory Assignment 1 in Inf-3203
//Use at your own risk :-)
//
//This code was adapted from: http://www.learningprocessing.com/examples/chapter-16/example-16-13/
//
//It will not work with images stored on HDFS without modification.
//And there is no indication about the progress of the motion estimation which could take a significant amount of time.
//
//Author: Joe Hurley joe@cs.uit.no

public class AutoProduce {

	public static void main(String[] args) throws IOException {
		String cam_one_dir = args[0];
		String cam_two_dir = args[1];
		
		//estimate camera one's motion
		ArrayList<Double> cam_one_motion = estimateMotion(cam_one_dir);
		//estimate camera two's motion
		ArrayList<Double> cam_two_motion = estimateMotion(cam_two_dir);
		
		//now compare the two arrays to pick the 'best' camera angle
		ArrayList<Integer> cam_selection = new ArrayList<Integer>(cam_one_motion.size());
		for (int i = 0; i < cam_one_motion.size(); i++) {
			if (cam_one_motion.get(i) > cam_two_motion.get(i)) { cam_selection.add(1); }
			else { cam_selection.add(2); }
		}
		
		//output the best camera to use for each moment of the event
		for (int i=0; i<cam_selection.size(); i++) {
			System.out.println(i + " : " + cam_selection.get(i));
			
		}
	}
	
	//read frames in sequence and generate a motion estimation for each frame
	//based on the difference between the color values of each pixel of this frame
	//and the same pixel of the previous frame
	public static ArrayList<Double> estimateMotion(String in_dir) throws IOException {
		
		File dir = new File(in_dir);
		BufferedImage frame = null;
		BufferedImage prevFrame = null;
		Color fcolor, pfcolor;
		int height = -1;
		int width = -1;
		
		ArrayList<Double> motion = new ArrayList<Double>();

		//make sure we are going to loop in sequential order
		File[] frameList = dir.listFiles();
		Arrays.sort(frameList, new Comparator<File>(){
			public int compare( File f1, File f2 )
			{
				return f1.getPath().compareTo(f2.getPath());
			}
		});
		
		for (File f : frameList) {
			
			//sanity check
			System.out.println("processing file: " + f.getPath());
			
			if (!f.getPath().endsWith("jpeg") && !f.getPath().endsWith("jpg")) continue;
			
			prevFrame = frame;
			frame = ImageIO.read(f);
			if (prevFrame==null) continue;

			if (height <0) { height = frame.getHeight(); }
			if (width <0) { width = frame.getWidth(); }
			
			int motionSum = 0;
			
			//compare each pixel in this frame to the same pixel in the previous frame
			for (int y=0; y<height; y++) {
				for (int x=0; x<width; x++) {
					
					fcolor = new Color(frame.getRGB(x, y));
					pfcolor = new Color(prevFrame.getRGB(x, y));
					
					motionSum+= Math.abs(fcolor.getRed() - pfcolor.getRed());
					motionSum+= Math.abs(fcolor.getGreen() - pfcolor.getGreen());
					motionSum+= Math.abs(fcolor.getBlue() - pfcolor.getBlue());
					
				}
			}
			
			//A higher number is caused by a significant change in the color values. 
			//This value is interpreted as the amount of motion between sequential frames.
			double motionEstimate = motionSum / ((double) height*width);
			motion.add(motionEstimate);
		}
		return motion;
	}

}
