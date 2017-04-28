package com.viveknk;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageRating {

public static class TokenizerMapper extends Mapper<Object, Text, Text, CompositeWritable>{
	
		/**
		 * <Object, Text, Text, CompositeWritable>
		 * 
		 * reads <Object,Text> as key and value
		 * 
		 * writes <Text,CompositeWritable> key-val pair as temporary output
		 * 
		 *  Note that the reduce must read <key,val> of this type
		 * 
		 */
		
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	
	    	String[] cols = value.toString().split(",");
	    	
	    	try {
	    		CompositeWritable val = new CompositeWritable(cols[0],cols[1],cols[2],cols[3]);
	    		
		    	val.name = val.name.toUpperCase();
	    		val.song = val.song.toUpperCase();
	    		
	    		if(val.rating>=9) {
	    			//writing <Text,CompositeWritable> as declared above
	    			context.write(new Text(val.name), val); //write null if it is not required
		    	}
	    	} catch(Exception ex) {
	    		ex.printStackTrace();
	    	}
	    	
	    }
	}

	public static class AvgRatingReducer extends Reducer<Text,CompositeWritable,Text,DoubleWritable> {
		
		/**
		 * 
		 * <Text,CompositeWritable,Text,DoubleWritable>
		 * 
		 * reads <Text,CompositeWritable> key-value as input 
		 * which is written by the Mapper function in the prev step
		 * 
		 * and writes the output of <Text,DoubleWritable>
		 * 
		 */
		
	    private DoubleWritable result = new DoubleWritable(0.0);
	    
	    public void reduce(Text key, Iterable<CompositeWritable> values, Context context ) throws IOException, InterruptedException {	      
	    	//reading Text, Collection of CompositeWritable as described above
	    	//The value is collection of the value type written by the map output
	    	//The elements with the same key is added to the same collection
	    	//and passed to the reducer
	    	
	    	double sum = 0.0;
	    	int count = 0;
	    	
	    	for (CompositeWritable val : values) {
	    		sum += val.rating;
	    		count++;
	    	}
	    	
	    	if(count>0) {
	    		//calculate the average
	    		result.set(sum/(double)count);
	    	}
	    	
	    	context.write(key, result); //write Text and DoubleWritable
	    }
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
	    
		Job job = Job.getInstance(conf, "CompositeWriteJob"); //configuration and job name
	    
	    job.setJarByClass(AverageRating.class);
	    
	    job.setMapperClass(TokenizerMapper.class);
	    
	    job.setReducerClass(AvgRatingReducer.class);
	    
	    //job.setNumReduceTasks(0); //if this is set reduce will not run
	    //and mapper will directly write the final output as part-m-* files
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(CompositeWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    
	    Path inPath = new Path(args[0]);
	    Path outPath = new Path(args[1]);
	    outPath.getFileSystem(conf).delete(outPath,true);
	    		
	    FileInputFormat.addInputPath(job, inPath);
	    FileOutputFormat.setOutputPath(job, outPath);
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}