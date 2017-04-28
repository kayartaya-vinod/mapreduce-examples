package com.viveknk;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CompositeFilter {

public static class TokenizerMapper extends Mapper<Object, Text, Object, CompositeWritable>{
		
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	
	    	String[] cols = value.toString().split(",");
	    	
	    	try {
	    		//from the input split by the comma delimiter and create the Writable Pojo instance
	    		CompositeWritable val = new CompositeWritable(cols[0],cols[1],cols[2],cols[3]);
	    		
	    		
	    		System.out.println();
		    	System.out.print(key+" ::: ");
		    	System.out.println(val);
		    	System.out.println();
		    	
		    	//filter out the entities with 9+ rating
	    		if(val.rating>=9) {
	    			context.write(null, val); //write null if key is not required
	    			//write the val object itself into the context as output
	    			//the toString() method of that object will be called while writing it into the file
		    	}
	    	} catch(Exception ex) {
	    		ex.printStackTrace();
	    	}
	    	
	    }
	}

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "CompositeWriteJob"); //configuration and job name
	    
	    //the class that is present in the jar file containing the Mapper and Reducer classes
	    job.setJarByClass(CompositeFilter.class);
	    
	    job.setMapperClass(TokenizerMapper.class);
	    job.setNumReduceTasks(0); //No reduce tasks.
	    
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    Path inPath = new Path(args[0]);
	    Path outPath = new Path(args[1]);
	    outPath.getFileSystem(conf).delete(outPath,true);
	    
	    FileInputFormat.addInputPath(job, inPath);
	    FileOutputFormat.setOutputPath(job, outPath);
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}