package com.mdsol.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.mdsol.search.InventoryItemWritable;

public class SearchMapReduce {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, InventoryItemWritable> {

		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, InventoryItemWritable> output,
				Reporter reporter) throws IOException {
			String line = value.toString();
			InventoryItemWritable inventoryItem = new InventoryItemWritable(
					line);
			output.collect(inventoryItem.getStatus(), inventoryItem);
		}
	}

	public static class Reduce extends MapReduceBase
			implements
			Reducer<IntWritable, InventoryItemWritable, IntWritable, NandanArrayWritable> {

		public void reduce(IntWritable key, Iterator<InventoryItemWritable> values,
				OutputCollector<IntWritable, NandanArrayWritable> output,
				Reporter reporter) throws IOException {
			NandanArrayWritable arrayWritable = new NandanArrayWritable(InventoryItemWritable.class);
			ArrayList<InventoryItemWritable> inventoryItemWritablesArrayList = new ArrayList<InventoryItemWritable>();
			
			InventoryItemWritable value = null;
			while (values.hasNext()) {
				value = values.next();
				inventoryItemWritablesArrayList.add(new InventoryItemWritable(value.getItemNumber().toString(), 
						value.getId().get(), value.getSequenceNumber().get(), value.getStatus().get()));
			}
			
			InventoryItemWritable[] inventoryItemWritablesArray = new InventoryItemWritable[inventoryItemWritablesArrayList.size()];
			arrayWritable.set(inventoryItemWritablesArrayList.toArray(inventoryItemWritablesArray));
			output.collect(key, arrayWritable);
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(SearchMapReduce.class);

		conf.setOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(InventoryItemWritable.class);
		conf.setOutputValueClass(NandanArrayWritable.class);

		conf.setMapperClass(Map.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}

}
