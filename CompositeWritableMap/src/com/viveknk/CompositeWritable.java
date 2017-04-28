package com.viveknk;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
 
//A composite POJO that implements the Writable interface

public class CompositeWritable implements Writable {

	long id;
	String name;
	String song;
	double rating;
	
	public CompositeWritable() {}
	
	public CompositeWritable(long id, String name, String song, double rating) {
		this.id = id;
		this.name = name;
		this.song = song;
		this.rating = rating;
	}
	
	public CompositeWritable(String id, String name, String song, String rating) {
		try {
			this.id = Long.parseLong(id);
			this.rating = Double.parseDouble(rating);
		} catch(Exception ex) {
			
		}
		this.name = name;
		this.song = song;
		
	}
	
	@Override
	public void readFields(DataInput inp) throws IOException {
		
		id = inp.readLong();
		name = WritableUtils.readString(inp);
		song = WritableUtils.readString(inp);
		rating = inp.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeLong(id);
		WritableUtils.writeString(out, name);
		WritableUtils.writeString(out, song);
		out.writeDouble(rating);
	}
	
	@Override
    public String toString() {
		
		return this.id + "," + this.name + "," + this.song + "," + this.rating;
	}
}