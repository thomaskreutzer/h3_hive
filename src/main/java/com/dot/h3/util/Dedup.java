package com.dot.h3.util;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Dedup {
	
	public ArrayList<Text> RemoveDupsText(ArrayList<Text> data){
		Set<Text> primesWithoutDuplicates = new LinkedHashSet<Text>(data);
		data.clear();
		data.addAll(primesWithoutDuplicates);
		return data;
	}
	
	public ArrayList<LongWritable> RemoveDupsLongWritable(ArrayList<LongWritable> data){
		Set<LongWritable> primesWithoutDuplicates = new LinkedHashSet<LongWritable>(data);
		data.clear();
		data.addAll(primesWithoutDuplicates);
		return data;
	}

	public ArrayList<String> RemoveDupsStr(ArrayList<String> data){
		Set<String> primesWithoutDuplicates = new LinkedHashSet<String>(data);
		data.clear();
		data.addAll(primesWithoutDuplicates);
		return data;
	}
	
	public ArrayList<Long> RemoveDupsLong(ArrayList<Long> data){
		Set<Long> primesWithoutDuplicates = new LinkedHashSet<Long>(data);
		data.clear();
		data.addAll(primesWithoutDuplicates);
		return data;
	}
	
}
