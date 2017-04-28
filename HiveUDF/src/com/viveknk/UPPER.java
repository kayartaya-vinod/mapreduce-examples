package com.viveknk;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class UPPER {

	public Text evaluate(Text input,Text input1) {
	    return new Text(input.toString().toUpperCase());
	}
}
