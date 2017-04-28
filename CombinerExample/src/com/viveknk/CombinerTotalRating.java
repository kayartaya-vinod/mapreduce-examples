package com.viveknk;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CombinerTotalRating {

public static class TokenizerMapper extends Mapper<Object, Text, LongWritable, CompositeWritable>{
		
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	
	    	String[] cols = value.toString().split(",");
	    	
	    	try {
	    		CompositeWritable val = new CompositeWritable(cols[0],cols[1],cols[2]);
	    		
	    		val.name = val.name.toString().toUpperCase();
	    		
	    		if(val.points>=35) {
	    			context.write(new LongWritable(val.id), val); //write null if it is not required
		    	}
	    	} catch(Exception ex) {
	    		ex.printStackTrace();
	    	}
	    	
	    }
	}
	


	//The output key-value pair should be same as the input key-value pair for a combiner
	//So, this combiner reads a <LongWritable,CompositeWritable> pair and also writes a <LongWritable,CompositeWritable> pair

	public static class AvgRatingCombiner extends Reducer<LongWritable,CompositeWritable,LongWritable,CompositeWritable> {
		
		CompositeWritable fval = new CompositeWritable(); 
		
	    public void reduce(LongWritable key, Iterable<CompositeWritable> values, Context context ) throws IOException, InterruptedException {	      
	    	
	    	double sum = 0.0;
	    	
	    	Iterator<CompositeWritable> it  = values.iterator();
	    	
	    	while(it.hasNext()) {
	    		fval = it.next();
	    		sum += fval.points;
	    	}
	    	
	    	fval.points = sum;
	    	
	    	context.write(key, fval);
	    }
	}

	
	//The output type of a reducer can be anything. But the input type must match that of the combiner it is reading from
	
	public static class AvgRatingReducer extends Reducer<LongWritable,CompositeWritable,LongWritable,DoubleWritable> {
		
	    private DoubleWritable result = new DoubleWritable(0.0);
	    
	    public void reduce(LongWritable key, Iterable<CompositeWritable> values, Context context) throws IOException, InterruptedException {	      
	    	
	    	double sum = 0.0;
	    	
	    	for (CompositeWritable val : values) {
	    		sum += val.points;
	    	}
	    	result.set(sum);
	    	context.write(key, result);
	    }
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "CombinerCompositeJob"); //configuration and job name
	    
	    job.setJarByClass(CombinerTotalRating.class);
	    
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(AvgRatingCombiner.class);
	    job.setReducerClass(AvgRatingReducer.class);
	    
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(CompositeWritable.class);
	    
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    
	    Path inPath = new Path(args[0]);
	    Path outPath = new Path(args[1]);
	    outPath.getFileSystem(conf).delete(outPath,true);
	    		
	    FileInputFormat.addInputPath(job, inPath);
	    FileOutputFormat.setOutputPath(job, outPath);
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}