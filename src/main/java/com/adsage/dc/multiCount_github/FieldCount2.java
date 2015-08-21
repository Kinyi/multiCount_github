package com.adsage.dc.multiCount_github;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 该文件是在测试服务器上执行用的
 * 根据算法计算出每天记录的概率
 * 测试在mr中使用json，然后根据记录中的字段在json中提取点击数,已完成
 * 进度：需要统计出某appid下所有记录的文件的行数,记为all_cli,已解决,在传进来的记录中拼接了该appid的点击量
 * 
 * @author chenqinyi
 *
 */
public class FieldCount2 {

	// 数据输入输出路径
	private static final String INPUT_PATH = "hdfs://172.16.3.151:9000/kinyi/result_zuobi_zhongjian";
	private static final String OUTPUT_PATH = "hdfs://172.16.3.151:9000/kinyi/result_zuobi";

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try {
			FileSystem fileSystem = FileSystem.get(conf);
			if (fileSystem.exists(new Path(OUTPUT_PATH))) {
				fileSystem.delete(new Path(OUTPUT_PATH), true);
			}

			String pathname = "/home/kinyi/bound";
			File file = new File(pathname);
			FileInputStream in = null;
			StringBuffer sb = new StringBuffer();
			String bound = "";
			try {
				in = new FileInputStream(file);
				byte[] b = new byte[100];
				while (in.read(b) != -1) {
					String string = new String(b);
					sb.append(string);
				}    
				// 读取文件获取每个字段具体值的上下界
				bound = sb.toString().trim();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (in != null) {
					try {
						in.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

			String pathname2 = "/home/kinyi/app_log";
			File file2 = new File(pathname2);
			FileInputStream in2 = null;
			StringBuffer sb2 = new StringBuffer();
			String jsonString = "";
			try {
				in = new FileInputStream(file2);
				byte[] by = new byte[100];
				while (in.read(by) != -1) {
					String string = new String(by);
					sb2.append(string);
				}
				// 读取文件获取json串
				jsonString = sb2.toString().trim();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (in2 != null) {
					try {
						in2.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

			//测试数据
//			 String jsonString =
//			 "{'46516':{'province':{'139450000':'10'},'model_type':{'4s':'2'},'system_version':{'4.1.2':'3'},'network_state':{'1':'4'},'is_jail_broken':{'1':'5'}}}";
			//把从文件获得的字符串内容用configuration传到mapper或reducer
			conf.set("jsonString", jsonString);
			conf.set("bound", bound);

			Job job = Job.getInstance(conf, FieldCount2.class.getSimpleName());
			job.setJarByClass(FieldCount2.class);
			job.setMapperClass(MyMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

//			job.setNumReduceTasks(0);

			job.setReducerClass(MyReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
			FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		HashMap<String, String> map = new HashMap<String, String>();

		/*
		 * 使用setup方法的目的是在执行map函数之前接收从文件读取的上下界内容来初始化一个map,供后续map函数读取每一行记录来从
		 * 这个map获取每个字段的上下界
		 */
		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String bound = conf.get("bound");
			//把文件内容按行切分
			String[] records = bound.split("\n");
			for (String record : records) {
				String[] split = record.split("\\s");
				if (split.length == 4) {
					//把字段名和具体值作为key输出
					String key = split[0] + "\t" + split[1];
					//把上界和下界作为value输出
					String value = split[2] + "\t" + split[3];
					map.put(key, value);
				}
			}
		}

		@Override
		protected void map(LongWritable k1, Text v1,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// 用于存储每行记录的五个字段分别对应的点击量
			ArrayList<String> valueList = new ArrayList<String>();
			// 接收传过来的json串
			Configuration configuration = context.getConfiguration();
			String jsonString = configuration.get("jsonString");
			// 解析json串时需要字段名，而记录本身不包含，所以需要手动构建一个数组，当字段不确定时，亦可作为参数传进
			String[] fieldArray = new String[] { "province", "model_type", "system_version", "network_state", "is_jail_broken" };
			String[] split = v1.toString().split("\\s");
			// json串的firstAttr
			String appid = split[0];
			//同一appid下的总点击量，在ExtractAppidRecord文件中拼接了
			String all_cli = split[6];
			for (int i = 1; i < split.length -1; i++) {
				// json串的secondAttr
				String field = fieldArray[i - 1];
				// json串的thirdAttr
				String key = split[i];
				//根据字段名和具体值来从保存上下界的map获取具体上下界
				String bound = map.get(field + "\t" + key);
				if (bound != null) {
					String[] bounds = bound.split("\t");
					//下界
					String downBound = bounds[0];
					//上界
					String upBound = bounds[1];
					//从json串中解析出具体的点击量
					long value = JsonUtils.parseThreeJsonObj(jsonString, appid, field, key);
					//把下界,点击量,上界,某appid的总点击量  组合
					String complete = downBound + "--" + value + "--" + upBound + "--" + all_cli;
					//把每条记录的五个字段都按上述格式组合后在存放在一个arraylist进行输出
					valueList.add(complete);
				}
			}
			context.write(new Text(appid), new Text(valueList.toString().substring(1, valueList.toString().length()-1)));
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text k2, Iterable<Text> v2s,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//把记录的字段名先存放在一个数组中,后续字段名有改变时可作为一个参数传进来
			String[] fea_arr = new String[] { "province", "model_type", "system_version", "network_state", "is_jail_broken"};
			double all_cli = 0;
			for (Text text : v2s) {
				String[] split = text.toString().split(",");
				Map<String, String> down = new HashMap<String, String>();
				Map<String, String> rec = new HashMap<String, String>();
				Map<String, String> upon = new HashMap<String, String>();
				for (int i = 0; i < split.length; i++) {
					String field = fea_arr[i];
					String[] values = split[i].split("--");
					//all_cli参数，供下面的方法使用
					all_cli = Double.parseDouble(values[3]);
					down.put(field, values[0]);
					rec.put(field, values[1]);
					upon.put(field, values[2]);
				}
				//在这里使用三个map
//				context.write(new Text(down.toString()+"-"+rec.toString()+"-"+upon.toString()), new Text(all_cli+""));
				//代码结合部分
				double union_prob = 1.0;
				double union_prob1 = 1.0;
				for (String key : fea_arr) {
					// System.out.println(key);
					double prob;
					double fea_upon = Double.parseDouble(upon.get(key));
					double fea_down = Double.parseDouble(down.get(key));
					double fea_window_cli = Double.parseDouble(rec.get(key));
					System.out.println(fea_upon + " " + fea_down + " " + fea_window_cli);
					prob = getProb(fea_window_cli, fea_upon, fea_down, all_cli);
					// System.out.println(prob);
					union_prob *= prob;
					union_prob1 *= (1 - prob);
				}
				double p = union_prob / (union_prob + union_prob1);
//				System.out.println(p);
				context.write(k2, new Text(p + ""));
			}
		}
		
		public double getProb(double fea_window_cli, double fea_upon,
				double fea_down, double all_cli) {
			double prob;
			if (fea_window_cli > fea_upon * all_cli) {
				prob = 0.5 + (fea_window_cli - fea_upon * all_cli) / (2 * all_cli);
			} else if (fea_window_cli < fea_down * all_cli) {
				prob = 0.5 - (fea_down * all_cli - fea_window_cli) / (2 * all_cli);
			} else {
				prob = 0.5;
			}
			return prob;
		}
	}
}
