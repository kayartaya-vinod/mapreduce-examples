package com.viveknk;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DistCache {

public static class EmpMapper extends Mapper<Object, Text, Object, Text>{
		
		//This collection is used to store the content of distributed cache file for further operations
	    Map<String,String> departments = new HashMap<String,String>();
	    
	    @Override
	    protected void setup(Context context) throws IOException,InterruptedException {
	    	
	    	//get the list of files added to distributed cache
	    	Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
	    	
	    	if(cacheFilesLocal != null) {
	    		//from each file, read the content
	    		for (Path eachPath : cacheFilesLocal) {
	    			try {
	    				File file = new File(eachPath.toString());
	    				BufferedReader br = new BufferedReader(new FileReader(file));
	    				String str = null;
	    				while((str=br.readLine())!=null) {
	    					//split them by comma and add to the department details hashmap
	    					String[] splits = str.split(",");
	    					departments.put(splits[0], splits[1]);
	    				}
	    				br.close();
	    			} catch(Exception ex) {
	    				ex.printStackTrace();
	    			}
	    		}
	    		//departments details are ready from all the distributed cache files
	    		System.out.println(departments);
	    	} else {
	    		System.out.println("No local cache files");
	    	}
	    }

	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	
	    	//printing the input read for debugging purposes
	    	System.out.println();
	    	System.out.print(key+" ::: ");
	    	System.out.println(value);
	    	System.out.println();
	    	
	    	//create an Employee object from the input data
	    	Employee emp = new Employee(value.toString());
	    	
	    	//replace the department id with the department name
	    	emp.setDept(departments.get(emp.getDept()));
	    	
	    	context.write(null, new Text(emp.toString())); //write null if key is not required
	    }
	}

	public static void main(String[] args) throws Exception {
		
		if(args.length<3) {
			System.err.println("need 3 args <input> <output> <path-to-dist-cache-file#reference>");
			System.exit(1);
		}
		
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "DistributedCacheJob"); //configuration and job name
	    
	    //put the smallest file in distributed cache
	    //By default, cache size is 10GB.
	    //If you want more memory configure "local.cache.size" in mapred-site.xml for any bigger/smaller value.
	    DistributedCache.addCacheFile(new URI(args[2]),job.getConfiguration());
	    
	    //the class that is present in the jar file containing the Mapper and Reducer classes
	    job.setJarByClass(DistCache.class);
	    
	    job.setMapperClass(EmpMapper.class); //setting the mapper class
	    job.setNumReduceTasks(0); //no reducer
	    
	    job.setOutputKeyClass(NullWritable.class); //we are ignoring the key
	    job.setOutputValueClass(Text.class); //we are writing the value as text
	    
	    Path inPath = new Path(args[0]);
	    Path outPath = new Path(args[1]);
	    outPath.getFileSystem(conf).delete(outPath,true); //deletes the output folder if already exists
	    
	    FileInputFormat.addInputPath(job, inPath);
	    FileOutputFormat.setOutputPath(job, outPath);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}