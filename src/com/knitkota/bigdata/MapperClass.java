package com.knitkota.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
	public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {

		try {

			String[] str = value.toString().split(",");

			if (str[4].equals("1")) {

				long duration = MainClass.getDifference(str[2], str[3]);

				context.write(new Text(str[0]), new LongWritable(duration));

			}

		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

}
