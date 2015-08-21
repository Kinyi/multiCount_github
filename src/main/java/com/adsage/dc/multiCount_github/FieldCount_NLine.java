package com.adsage.dc.multiCount_github;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 根据算法计算出每天记录的概率 测试在mr中使用json，然后根据记录中的字段在json中提取点击数,已完成
 * 进度：需要统计出某appid下所有记录的文件的行数,记为all_cli,已解决,在传进来的记录中拼接了该appid的点击量
 * 
 * @author chenqinyi
 *
 */
public class FieldCount_NLine extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// 参数为appidList的文件名
		String appidListName = args[0];

		// 数据输入输出路径
		String INPUT_PATH = "hdfs://172.17.60.101:9000/kinyi/file/fieldList" + appidListName;
		String OUTPUT_PATH = "hdfs://172.17.60.101:9000/kinyi/result" + appidListName;

		Configuration conf = new Configuration();
		conf.set("appidListName", appidListName);
		conf.setInt("mapreduce.input.lineinputformat.linespermap", 100000);

		try {
			FileSystem fileSystem = FileSystem.get(conf);
			if (fileSystem.exists(new Path(OUTPUT_PATH))) {
				fileSystem.delete(new Path(OUTPUT_PATH), true);
			}

			Job job = Job.getInstance(conf, FieldCount_NLine.class.getSimpleName());
			job.setJarByClass(FieldCount_NLine.class);
			
			job.setInputFormatClass(NLineInputFormat.class);
//			NLineInputFormat.setNumLinesPerSplit(job, 100000);
			
			job.setMapperClass(MyMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setReducerClass(MyReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
			FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new FieldCount_NLine(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		HashMap<String, String> map = new HashMap<String, String>();
		String jsonString;
		String bound;

		/*
		 * 使用setup方法的目的是在执行map函数之前接收从文件读取的上下界内容来初始化一个map,供后续map函数读取每一行记录来从
		 * 这个map获取每个字段的上下界
		 */
		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String appidListName = conf.get("appidListName");

			// 首先把json串文件上传到hdfs中，然后在setup里读取该hdfs文件保存在节点的文件中，然后再从文件中读取数据存放在字符串中
			FileSystem fileSystem = null;
			try {
				fileSystem = FileSystem.get(new URI(
						"hdfs://172.17.60.101:9000/"), new Configuration());
			} catch (URISyntaxException e1) {
				e1.printStackTrace();
			}

			FSDataInputStream in_json = fileSystem.open(new Path(
					"/kinyi/app_log" + appidListName));
			File file_json = new File("/tmp/json" + appidListName);
			FileOutputStream fos_json = new FileOutputStream(file_json);
			IOUtils.copyBytes(in_json, fos_json, 1024, true);

			String pathname2 = "/tmp/json" + appidListName;
			File file2 = new File(pathname2);
			FileInputStream in2 = null;
			StringBuffer sb2 = new StringBuffer();
			try {
				in2 = new FileInputStream(file2);
				byte[] by = new byte[100];
				while (in2.read(by) != -1) {
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
			// 首先把上下界文件上传到hdfs中，然后在setup里读取该hdfs文件保存在节点的文件中，然后再从文件中读取数据存放在字符串中
			FileSystem fileSystem_bound = null;
			try {
				fileSystem_bound = FileSystem.get(new URI(
						"hdfs://172.17.60.101:9000/"), new Configuration());
			} catch (URISyntaxException e1) {
				e1.printStackTrace();
			}
			FSDataInputStream in_bound = fileSystem_bound.open(new Path(
					"/kinyi/bound"));
			File file_bound = new File("/tmp/json");
			FileOutputStream fos_bound = new FileOutputStream(file_bound);
			IOUtils.copyBytes(in_bound, fos_bound, 1024, true);

			String pathname_bound = "/tmp/json";
			File file = new File(pathname_bound);
			FileInputStream inbound = null;
			StringBuffer sb_bound = new StringBuffer();
			try {
				inbound = new FileInputStream(file);
				byte[] by_bound = new byte[100];
				while (inbound.read(by_bound) != -1) {
					String string_bound = new String(by_bound);
					sb_bound.append(string_bound);
				}
				// 读取文件获取上下界文件
				bound = sb_bound.toString().trim();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (inbound != null) {
					try {
						inbound.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

			/*
			 * 下面注释这一段是避免从hdfs中的文件下载内容到本地文件中再从本地文件中读取数据，而是把outputStream直接转化为字符串来使用
			 * ，效率应该会更高
			 * //首先把json串文件上传到hdfs中，然后在setup里读取该hdfs文件保存在节点的文件中，然后再从文件中读取数据存放在字符串中
			 * FileSystem fileSystem = null; try { fileSystem =
			 * FileSystem.get(new URI("hdfs://172.17.60.101:9000/"),new
			 * Configuration()); } catch (URISyntaxException e1) {
			 * e1.printStackTrace(); }
			 * 
			 * FSDataInputStream in_json = fileSystem.open(new
			 * Path("/kinyi/app_log" + appidListName)); ByteArrayOutputStream
			 * baos_json = new ByteArrayOutputStream();
			 * IOUtils.copyBytes(in_json, baos_json, 1024, true);
			 * 
			 * jsonString = baos_json.toString();
			 * 
			 * //首先把上下界文件上传到hdfs中，然后在setup里读取该hdfs文件保存在节点的文件中，然后再从文件中读取数据存放在字符串中
			 * FileSystem fileSystem_bound = null; try { fileSystem_bound =
			 * FileSystem.get(new URI("hdfs://172.17.60.101:9000/"),new
			 * Configuration()); } catch (URISyntaxException e1) {
			 * e1.printStackTrace(); } FSDataInputStream in_bound =
			 * fileSystem_bound.open(new Path("/kinyi/bound"));
			 * ByteArrayOutputStream baos_bound = new ByteArrayOutputStream();
			 * IOUtils.copyBytes(in_bound, baos_bound, 1024, true);
			 * 
			 * bound = baos_bound.toString();
			 */

			// 处理存储上下界文件的字符串，解析成一个map
			String[] records = bound.split("\n");
			for (String record : records) {
				String[] split = record.split("\\s");
				if (split.length == 4) {
					// 把字段名和具体值作为key输出
					String key = split[0] + "\t" + split[1];
					// 把上界和下界作为value输出
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
			// 解析json串时需要字段名，而记录本身不包含，所以需要手动构建一个数组，当字段不确定时，亦可作为参数传进
			String[] fieldArray = new String[] { "province", "model_type",
					"system_version", "network_state", "is_jail_broken","operator" };
			String[] split = v1.toString().split("\\s");
			// json串的firstAttr
			String appid = split[0];
			// 同一appid下的总点击量，在ExtractAppidRecord文件中拼接了
			String all_cli = split[7];
			for (int i = 1; i < split.length - 1; i++) {
				// json串的secondAttr
				String field = fieldArray[i - 1];
				// json串的thirdAttr
				String key = split[i];
				// 根据字段名和具体值来从保存上下界的map获取具体上下界
				String bound = map.get(field + "\t" + key);
				String complete = "";
				if (bound != null) {
					String[] bounds = bound.split("\t");
					// 下界
					String downBound = bounds[0];
					// 上界
					String upBound = bounds[1];
					// 从json串中解析出具体的点击量
					long value = JsonUtils.parseThreeJsonObj(jsonString, appid,	field, key);
					// 把下界,点击量,上界,某appid的总点击量 组合
					complete = downBound + "--" + value + "--" + upBound + "--"	+ all_cli + "--" + key.replaceAll(",", ".");
					// 把每条记录的五个字段都按上述格式组合后在存放在一个arraylist进行输出
					valueList.add(complete);
				} else { // 后修改部分
					complete = "***";
					valueList.add(complete);
				}
			}
			context.write(new Text(appid), new Text(valueList.toString().substring(1, valueList.toString().length() - 1)));
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text k2, Iterable<Text> v2s,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// 把记录的字段名先存放在一个数组中,后续字段名有改变时可作为一个参数传进来
			String[] fea_arr = new String[] { "province", "model_type",
					"system_version", "network_state", "is_jail_broken","operator" };
			double all_cli = 0;
			double exceptionProb = 0;

			for (Text text : v2s) {
				String[] split = text.toString().split(",");
				Map<String, String> down = new HashMap<String, String>();
				Map<String, String> rec = new HashMap<String, String>();
				Map<String, String> upon = new HashMap<String, String>();
				Map<String, String> fields = new HashMap<String, String>();
				for (int i = 0; i < split.length; i++) {
					String field = fea_arr[i];
					String oneField = split[i];
					if (!oneField.contains("***")) {
						String[] values = oneField.split("--");
						if (values.length == 5) {
							// all_cli参数，供下面的方法使用
							all_cli = Double.parseDouble(values[3]);
							down.put(field, values[0]);
							rec.put(field, values[1]);
							upon.put(field, values[2]);
							fields.put(field, values[4]);
						}
					} else {
						exceptionProb = 0.5;
					}
				}
				// 在这里使用三个map
				// context.write(new
				// Text(down.toString()+"-"+rec.toString()+"-"+upon.toString()),
				// new Text(all_cli+""));
				// 代码结合部分
				double union_prob = 1.0;
				double union_prob1 = 1.0;
				String compound = "";
				for (String key : fea_arr) {
					// System.out.println(key);
					double prob;
					String jsonValue = rec.get(key);
					double fea_upon = Double.parseDouble(upon.get(key));
					double fea_down = Double.parseDouble(down.get(key));
					double fea_window_cli = Double.parseDouble(jsonValue);
					String field;
					if ("model_type".equals(key)) {
						field = fields.get(key).replaceAll("\\.", ",");
					} else {
						field = fields.get(key);
					}
					System.out.println(fea_upon + " " + fea_down + " "	+ fea_window_cli);
					prob = (exceptionProb == 0.5 ? 0.5 : getProb(fea_window_cli, fea_upon, fea_down, all_cli));
					compound += field + "*" + prob + "*" + jsonValue + "/"	+ all_cli + "\t";
					// System.out.println(prob);
					union_prob *= prob;
					union_prob1 *= (1 - prob);
				}
				double p = union_prob / (union_prob + union_prob1);
				// System.out.println(p);
				context.write(k2, new Text(compound + "" + p));
			}
		}

		public double getProb(double fea_window_cli, double fea_upon,
				double fea_down, double all_cli) {
			double prob;
			/**
			 * if(fea_upon*all_cli<0.1){ prob=0.5; }
			 */
			if (fea_window_cli > fea_upon * all_cli) {
				prob = 0.5 + (fea_window_cli - fea_upon * all_cli)
						/ (2 * all_cli);
			} else if (fea_window_cli < fea_down * all_cli) {
				prob = 0.5 - (fea_down * all_cli - fea_window_cli)
						/ (2 * all_cli);
			} else {
				prob = 0.5;
			}
			return prob;
		}
	}
}

// xuran's code
class GetProb {
	public static void main(String[] args) {
		GetProb com1 = new GetProb();
		Map<String, String> rec = new HashMap<String, String>();
		rec.put("province", "85");
		rec.put("model_type", "3000");
		rec.put("system_version", "726");
		rec.put("network_state", "3000");
		int all_cli = 3000;
		// 上界
		Map<String, String> upon = new HashMap<String, String>();
		upon.put("province", "0.0209622");
		upon.put("model_type", "0.225322");
		upon.put("system_version", "0.0113591");
		upon.put("network_state", "0.830209");
		// 下界
		Map<String, String> down = new HashMap<String, String>();
		down.put("province", "0.0209358");
		down.put("model_type", "0.124612");
		down.put("system_version", "0.0110269");
		down.put("network_state", "0.828703");

		String[] fea_arr = { "province", "model_type", "system_version",
				"network_state" };
		double union_prob = 1.0;
		double union_prob1 = 1.0;
		for (String key : fea_arr) {
			// System.out.println(key);
			double prob;
			double fea_upon = Double.parseDouble(upon.get(key));
			double fea_down = Double.parseDouble(down.get(key));
			double fea_window_cli = Double.parseDouble(rec.get(key));
			System.out
					.println(fea_upon + " " + fea_down + " " + fea_window_cli);
			prob = com1.getProb(fea_window_cli, fea_upon, fea_down, all_cli);
			// System.out.println(prob);
			union_prob *= prob;
			union_prob1 *= (1 - prob);
		}
		double p = union_prob / (union_prob + union_prob1);
		System.out.println(p);
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
