package com.viveknk;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CombinerAverageRating {

public static class TokenizerMapper extends Mapper<Object, Text, LongWritable, CompositeWritable> {
	
		/**
		 * 
		 * <Object, Text, LongWritable, CompositeWritable>
		 * 
		 * Mapper reads <Object, Text> and writes <LongWritable, CompositeWritable>
		 * 
		 * 
		 */
		
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	
	    	String[] cols = value.toString().split(",");
	    	
	    	try {
	    		CompositeWritable val = new CompositeWritable(cols[0],cols[1],cols[2]);
	    		
	    		//convert the name to uppercase
		    	val.name = val.name.toUpperCase();
	    		
		    	//filter the entities with points greater than or equal to 35
	    		if(val.points>=35) {
	    			context.write(new LongWritable(val.id), val); //write null if it is not required
		    	}
	    	} catch(Exception ex) {
	    		ex.printStackTrace();
	    	}
	    	
	    }
	}

	
	public static class AvgRatingCombiner extends Reducer<LongWritable,CompositeWritable,LongWritable,CompositeWritable> {
		
		CompositeWritable fval = new CompositeWritable();
		
		/**
		 * 
		 * The combiner reads <LongWritable,CompositeWritable>
		 * 
		 * and writes <LongWritable,CompositeWritable> according to the rule
		 * 
		 */
	    public void reduce(LongWritable key, Iterable<CompositeWritable> values, Context context ) throws IOException, InterruptedException {	      
	    	
	    	double sum = 0.0;
	    	int count = 0;
	    	
	    	Iterator<CompositeWritable> it  = values.iterator();
	    	
	    	while(it.hasNext()) {
	    		fval = it.next();
	    		sum += fval.points;
	    		count++;
	    	}
	    	
	    	if(count>0) {
	    		fval.points = sum;
	    		fval.count = count;
	    		context.write(key, fval);
	    	}
	    }
	}

	public static class AvgRatingReducer extends Reducer<LongWritable,CompositeWritable,LongWritable,CompositeWritable> {
		
		/**
		 * 
		 * <LongWritable,CompositeWritable,LongWritable,CompositeWritable>
		 * 
		 * reads <LongWritable,CompositeWritable> which is same as the output of the combiner/mapper
		 * 
		 */
		
		CompositeWritable fval = new CompositeWritable();
		
	    public void reduce(LongWritable key, Iterable<CompositeWritable> values, Context context ) throws IOException, InterruptedException {	      
	    	
	    	double sum = 0.0;
	    	int count = 0;
	    	
	    	Iterator<CompositeWritable> it  = values.iterator();
	    	
	    	while(it.hasNext()) {
	    		fval = it.next();
	    		sum += fval.points;
	    		count += fval.count;
	    	}
	    	
	    	if(count>0) {
	    		double avg = sum/(double)count;
	    		fval.points = avg;
	    		fval.count = -1;
	    		context.write(key, fval);
	    	}
	    }
	    
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "CombinerCompositeJob"); //configuration and job name
	    
	    job.setJarByClass(CombinerAverageRating.class);
	    
	    job.setMapperClass(TokenizerMapper.class); //setting mapper class
	    job.setCombinerClass(AvgRatingCombiner.class); //setting combiner class
	    job.setReducerClass(AvgRatingReducer.class); //setting reducer class 
	    
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(CompositeWritable.class);
	    
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(CompositeWritable.class);
	    
	    Path inPath = new Path(args[0]);
	    Path outPath = new Path(args[1]);
	    outPath.getFileSystem(conf).delete(outPath,true);
	    		
	    FileInputFormat.addInputPath(job, inPath);
	    FileOutputFormat.setOutputPath(job, outPath);
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}