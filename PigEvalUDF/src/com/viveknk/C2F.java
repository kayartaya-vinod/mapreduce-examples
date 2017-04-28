package com.viveknk;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

//UDF to convert Celsius to Fahrenheit
public class C2F extends EvalFunc<Double> {

	public Double exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try{
            String str = (String)input.get(0);
            double celsiusVal = Double.parseDouble(str);
            return (celsiusVal*1.8)+32;
        } catch(NumberFormatException e){
        	System.err.println("WARN: C2F: failed. Input not a valid number - " + e.getMessage());
            return null;
        } catch(Exception e){
            System.err.println("WARN: C2F: failed to process input; error - " + e.getMessage());
            return null;
        }
    }
}
