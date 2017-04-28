package com.viveknk;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Filter {

	public static class TokenizerMapper extends Mapper<Object, Text, Object, Text>{
	
		/**
		 * The Generic order is as follows:
		 * 
		 * <Object, Text, Object, Text> = <InputKey, InputValue, OutputKey, OutputValue>
		 * 
		 * The keys must implement WriteComparable
		 * The values must implement Writable
		 * 
		 */
	
		private Text word = new Text();

	    /*
	     * this the map function that needs to be overridden to implement our mapping functionality
	     * this is executed for every line of input file(s)
	     * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	     */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	
	    	//printing the input for debugging purposes
	    	System.out.println();
	    	System.out.print(key+" ::: ");
	    	System.out.println(value);
	    	System.out.println();
	    	
	    	//splitting the input into tokens with space as delimiter
	    	//StringTokenizer splits a string based on space character
	    	StringTokenizer itr = new StringTokenizer(value.toString());
	    	
	    	//for each word
	    	while (itr.hasMoreTokens()) {
	    	  
	    	  String wd = itr.nextToken();
	    	  //we check if the if the word contains the string "do"
	    	  if(wd.contains("do")) {
	    		  word.set(wd);
	    		  //if it contains we write the word into the output
	    		  context.write(null, word); //write null if it is not required
	    		  //else ignore that word
	    	  }
	    	}
	    }
	}
	
	public static void main(String[] args) throws Exception {
		
		//Loading the default configuration
		Configuration conf = new Configuration();
		
		//creating a job with that configuration
	    Job job = Job.getInstance(conf, "MapOnlyJob"); //configuration and job name
	    
	    //set the class that is int the jar that contains the Mapper and Reducer classes
	    //This method sets the jar file in which each node will look for the Mapper and Reducer classes.
	    //It does not create a jar from the given class.
	    //Rather, it identifies the jar containing the given class.
	    //And yes, that jar file is "executed" (actually the Mapper and Reducer in that jar file are executed) for the MapReduce job.
	    job.setJarByClass(Filter.class);
	    
	    //set the mapper class
	    job.setMapperClass(TokenizerMapper.class);
	    
	    //set the number of reduce to "0" if it is a Map only job
	    job.setNumReduceTasks(0);
	    //and mapper will directly write the final output as part-m-* files
	    //when this is non-zero, the final output is written only by the reducer 
	    //and therefore mapper is not allowed to write the final output
	    
	    //set the output key type
	    job.setOutputKeyClass(NullWritable.class);
	    //set the output value type
	    job.setOutputValueClass(Text.class);
	    
	    //input and output path as given by the user as command line arguments
	    Path inPath = new Path(args[0]);
	    Path outPath = new Path(args[1]);
	    outPath.getFileSystem(conf).delete(outPath,true); //deleting the output folder if it exists already
	    
	    FileInputFormat.addInputPath(job, inPath); //setting the inputs path to this job
	    FileOutputFormat.setOutputPath(job, outPath); //setting the outputs path to this job
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}