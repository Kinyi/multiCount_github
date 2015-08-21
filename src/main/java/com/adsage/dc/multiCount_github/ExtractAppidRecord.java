package com.adsage.dc.multiCount_github;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 根据给定的appid列表从数据文件中提取包含这些appid的记录
 * 
 * @author chenqinyi
 *
 */
public class ExtractAppidRecord extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		//第一个参数为日期
		//String dateString = args[0];
		//第二个参数为appidList的文件名
		String appidListName = args[0];

		// 数据输入输出路径
		String INPUT_PATH = "hdfs://172.17.60.101:9000/kinyi/fullData"/* + dateString*/;
		String INPUT_PATH2 = "hdfs://172.17.60.101:9000/kinyi/" + appidListName;
		String OUTPUT_PATH = "hdfs://172.17.60.101:9000/kinyi/fieldList" + appidListName;

		Configuration conf = new Configuration();
		try {
			FileSystem fileSystem = FileSystem.get(conf);
			if (fileSystem.exists(new Path(OUTPUT_PATH))) {
				fileSystem.delete(new Path(OUTPUT_PATH), true);
			}
			Job job = Job.getInstance(conf,
					ExtractAppidRecord.class.getSimpleName());
			job.setJarByClass(ExtractAppidRecord.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setReducerClass(MyReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			MultipleInputs.addInputPath(job, new Path(INPUT_PATH),
					TextInputFormat.class, MyMapper.class);
			MultipleInputs.addInputPath(job, new Path(INPUT_PATH2),
					TextInputFormat.class, MyMapper2.class);
			FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new ExtractAppidRecord(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		// 传进来的每一行的数据格式为：appid,click_date,province,model_type,system_version,network_state,is_jail_broken,operator
		@Override
		protected void map(LongWritable k1, Text v1,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = v1.toString().split("\\s");
			// 把appid作为key
			String k2 = split[0];
			// 把province,model_type,system_version,network_state,is_jail_broken,operator这六个字段作为value
			String v2 = split[2] + "\t" + split[3] + "\t" + split[4] + "\t"	+ split[5] + "\t" + split[6] + "\t" + split[7];
			context.write(new Text(k2), new Text(v2));
		}
	}

	public static class MyMapper2 extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable k1, Text v1,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// appid作为key,value打标签
			context.write(v1, new Text("*"));
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text k2, Iterable<Text> v2s,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			ArrayList<String> arrayList = new ArrayList<String>();
			// 把迭代器的元素放进一个ArrayList中,用于后续的判断和遍历
			for (Text text : v2s) {
				arrayList.add(text.toString());
			}
			// 判断集合中是否有打标签,如果有则为appid list文件中的appid
			if (arrayList.contains("*")) {
				for (String string : arrayList) {
					// 当元素不为*时才进行处理
					if (!"*".equals(string)) {
						// arrayList的长度为同一appid的记录的条数
						long length = arrayList.size() - 1;
						// 以appid为key,value为该条记录拼接上该appid的记录条数
						context.write(k2, new Text(string + "\t" + length));
					}
				}
			}
		}
	}
}
