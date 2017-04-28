package com.viveknk;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeWritable implements Writable {

	long id;
	String name;
	double points;
	long count = 1;
	
	public CompositeWritable() {}
	
	public CompositeWritable(long id, String name, double points,long count) {
		this.id = id;
		this.name = name;
		this.points = points;
		this.count = count;
	}
	
	public CompositeWritable(long id, String name, double points) {
		this.id = id;
		this.name = name;
		this.points = points;
	}
	
	public CompositeWritable(String id, String name, String points, String count) {
		try {
			this.id = Long.parseLong(id.trim());
		} catch(Exception ex) {
			
		}
		this.name = name;
		try {
			this.points = Double.parseDouble(points.trim());
		} catch(Exception ex) {
			
		}
		try {
			this.count = Long.parseLong(count.trim());
		} catch(Exception ex) {
			
		}
	}
	
	public CompositeWritable(String id, String name, String points) {
		try {
			this.id = Long.parseLong(id.trim());
		} catch(Exception ex) {
			
		}
		this.name = name;
		try {
			this.points = Double.parseDouble(points.trim());
		} catch(Exception ex) {
			
		}
	}
	
	@Override
	public void readFields(DataInput inp) throws IOException {
		
		id = inp.readLong();
		name = WritableUtils.readString(inp);
		points = inp.readDouble();
		count = inp.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeLong(id);
		WritableUtils.writeString(out, name);
		out.writeDouble(points);
		out.writeLong(count);
	}
	
	@Override
    public String toString() {
		
		return this.id + "," + this.name + "," + this.points + (count>0?("," + count):"");
	}
}