package com.rahul.sequencewriter;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SequenceWrite {

	private static final String[] DATA = {
		"One, two, buckle my shoe",
		"Three, four, shut the door",
		"Five, six, pick up sticks",
		"Seven, eight, lay them straight",
		"Nine, ten, a big fat hen"
		};
	
	public static void main(String[] args) throws IOException {
		Path path = new Path("hdfs://localhost:54310/user/hadoop/sequencefile");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:54310/user/hadoop"), conf);
		
		// Sequence Key and Values
		IntWritable key = new IntWritable();
		Text value = new Text();
		
		SequenceFile.Writer writer = null;
		
		writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, Text.class);
		
		for (int i = 0; i < 100; i++) {
			key.set(100 - i);
			value.set(DATA[i % DATA.length]);
			System.out.println("[" + writer.getLength() + "]\t" + key + "\t" + value);
			writer.append(key, value);
		}
	}
}
