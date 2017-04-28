package com.viveknk;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

//UDF to check if the second argument is a factor of first argument.
//both arguments are integers
public class isFactor extends FilterFunc {

	public Boolean exec(Tuple input) throws IOException {
		
        if (input == null || input.size() == 0)
            return null;
        try{
            String number = (String)input.get(0);
            String factor = (String)input.get(1);
            int n = Integer.parseInt(number);
            int f = Integer.parseInt(factor);
            return n%f==0;
        } catch(NumberFormatException e){
        	System.err.println("WARN: isFactor: failed. Inputs not a valid integers - " + e.getMessage());
            return null;
        } catch(Exception e){
            System.err.println("WARN: isFactor: failed to process input; error - " + e.getMessage());
            return null;
        }
    }
}
