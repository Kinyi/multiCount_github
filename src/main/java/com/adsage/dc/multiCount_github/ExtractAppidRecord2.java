package com.adsage.dc.multiCount_github;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 该文件是在测试服务器上执行用的
 * 根据给定的appid列表从数据文件中提取包含这些appid的记录
 * @author chenqinyi
 *
 */
public class ExtractAppidRecord2 {

	//数据输入输出路径
	private static final String INPUT_PATH = "hdfs://172.16.3.151:9000/kinyi/tempdata";
	private static final String INPUT_PATH2 = "hdfs://172.16.3.151:9000/kinyi/zuobi";
	private static final String OUTPUT_PATH = "hdfs://172.16.3.151:9000/kinyi/result_zuobi_zhongjian";

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try {
			FileSystem fileSystem = FileSystem.get(conf);
			if (fileSystem.exists(new Path(OUTPUT_PATH))) {
				fileSystem.delete(new Path(OUTPUT_PATH), true);
			}
			Job job = Job.getInstance(conf, ExtractAppidRecord2.class.getSimpleName());
			job.setJarByClass(ExtractAppidRecord2.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setReducerClass(MyReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			MultipleInputs.addInputPath(job, new Path(INPUT_PATH), TextInputFormat.class, MyMapper.class);
			MultipleInputs.addInputPath(job, new Path(INPUT_PATH2), TextInputFormat.class, MyMapper2.class);
			FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		

		@Override
		protected void map(LongWritable k1, Text v1,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = v1.toString().split("\\s");
			String k2 = split[0];
			String v2 = split[1] + "\t" + split[2] + "\t" + split[3] + "\t" + split[4] + "\t" + split[5];
			context.write(new Text(k2), new Text(v2));
		}
	}
	
	public static class MyMapper2 extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable k1, Text v1,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(v1, new Text("*"));
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text k2, Iterable<Text> v2s,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			ArrayList<String> arrayList = new ArrayList<String>();
			for (Text text : v2s) {
				arrayList.add(text.toString());
			}
			if (arrayList.contains("*")) {
				for (String string : arrayList) {
					if (!"*".equals(string)) {
						long length = arrayList.size();
						context.write(k2, new Text(string + "\t" + length));
					}
				}
			}
		}
	}
}
