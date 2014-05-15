package com.mdsol.search;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class NandanArrayWritable extends ArrayWritable {

	public NandanArrayWritable(Class<? extends Writable> valueClass) {
		super(valueClass);
	}

	public String toString() {
		Writable [] values = get();
		
		//String[] strings = new String[values.length];
		StringBuffer strBuf = new StringBuffer();
		for (int i = 0; i < values.length; i++) {
			strBuf.append(values[i].toString());
		}
		return strBuf.toString();
	}

}
