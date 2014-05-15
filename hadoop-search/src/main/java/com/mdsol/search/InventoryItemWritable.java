package com.mdsol.search;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.io.WritableComparable;

public class InventoryItemWritable implements Writable {

	private Text itemNumber;
	private IntWritable id, sequenceNumber, status;
    
	public InventoryItemWritable() {
    	this.itemNumber = new Text();
    	this.id = new IntWritable(); 
    	this.sequenceNumber =  new IntWritable();   
    	this.status = new IntWritable(); 
    } 
	
	public InventoryItemWritable(Text itemNumber, IntWritable id, IntWritable sequenceNumber, IntWritable status) {
    	this.itemNumber = itemNumber;
    	this.id = id; 
    	this.sequenceNumber =  sequenceNumber;   
    	this.status = status; 
    } 
    
	public Text getItemNumber() {
		return itemNumber;
	}

	public void setItemNumber(Text itemNumber) {
		this.itemNumber = itemNumber;
	}

	public IntWritable getId() {
		return id;
	}

	public void setId(IntWritable id) {
		this.id = id;
	}

	public IntWritable getStatus() {
		return status;
	}

	public void setStatus(IntWritable status) {
		this.status = status;
	}
	
	public IntWritable getSequenceNumber() {
		return sequenceNumber;
	}

	public void setSequenceNumber(IntWritable sequenceNumber) {
		this.sequenceNumber = sequenceNumber;
	}

	public InventoryItemWritable(String line) {
		super();
		StringTokenizer tokenizer = new StringTokenizer(line, ",");
		if (tokenizer.hasMoreTokens()) {
			int id = Integer.parseInt(tokenizer.nextToken());
			this.id = new IntWritable(id);
		}
		if (tokenizer.hasMoreTokens()) {
			this.itemNumber = new Text();
			itemNumber.set(tokenizer.nextToken());
		}
		if (tokenizer.hasMoreTokens()) {
			int seqNum = Integer.parseInt(tokenizer.nextToken());
			this.sequenceNumber = new IntWritable(seqNum);
		}
		if (tokenizer.hasMoreTokens()) {
			int status = Integer.parseInt(tokenizer.nextToken());
			this.status = new IntWritable(status);
		}
	}

   public String toString() {
 	 return  "id:[" + this.id.toString() + 
 	   "], item_number:[" + 
 	   this.itemNumber.toString() + "], sequence_number:[" +
 	  this.sequenceNumber.toString() + "], status:[" + this.status.toString() + "]"; 
   }

	public void readFields(DataInput in) throws IOException {
		id.readFields(in);
		itemNumber.readFields(in);
		sequenceNumber.readFields(in);
		status.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		id.write(out);
		itemNumber.write(out);
		sequenceNumber.write(out);
		status.write(out);
	}
	
//	 public int compareTo(InventoryItemWritable o) {
//         return (this.id.get() < o.id.get() ? -1 : (this.id.get() == o.id.get() ? 0 : 1));
//     }
//	 
//     public int hashCode() {
//    	 return this.id.get();
//     }

}
