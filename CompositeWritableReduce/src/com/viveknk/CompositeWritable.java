package com.viveknk;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

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
			this.id = Long.parseLong(id.trim());
		} catch(Exception ex) {
			
		}
		this.name = name;
		this.song = song;
		try {
			this.rating = Double.parseDouble(rating.trim());
		} catch(Exception ex) {
			
		}
		
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