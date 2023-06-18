package com.knitkota.bigdata;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class MainClass {

	public static void main(String[] args) throws Exception {

		

			Configuration conf = new Configuration();
			// conf.set("name", "value")
			// conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
			Job job = Job.getInstance(conf, "Assigenment One");
			job.setJarByClass(MainClass.class);
			job.setMapperClass(MapperClass.class);
			// job.setCombinerClass(ReduceClass.class);
			job.setReducerClass(ReducerClass.class);
			job.setNumReduceTasks(1);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));

			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);

		

	}

	public static long getDifference(String startTime, String endTime) {
		String pattern = "yyyy-MM-dd HH:mm:ss";

		try {
			SimpleDateFormat sdf = new SimpleDateFormat(pattern);
			Date dateStart = sdf.parse(startTime);

			Date dateEnd = sdf.parse(endTime);

			long durationInMilli = dateEnd.getTime() - dateStart.getTime();

			long durationInMinutes = durationInMilli / (1000 * 60);

			return durationInMinutes;
		} catch (ParseException e) {
			e.printStackTrace();
		}

		return 0;
	}

}
