package com.adsage.dc.multiCount_github;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.tools.ant.taskdefs.Length;
import org.codehaus.jettison.json.JSONException;




public class Test {

	@org.junit.Test
	public void test() throws JSONException {
		/*String jsonString = "{\"46516\":{\"province\":{\"139450000\":100},\"model_type\":{'4s':200},\"system_version\":{\"4.1.2\":300},\"network_state\":{\"1\":400},\"is_jail_broken\":{\"1\":500}}}";
		long result = JsonUtils.parseThreeJsonObj(jsonString, "46516", "model_type", "4s");
		System.out.println(result);*/
		
		/*ArrayList<String> arrayList = new ArrayList<String>();
		arrayList.add("kinyi");
		arrayList.add("martin");
		arrayList.add("allen");
		
		String first = arrayList.get(2);
		System.out.println(first);
		String result = arrayList.toString();
		System.out.println(result);*/
		
		//读取文件
		/*String pathname = "d:/test.txt";
		File file = new File(pathname);
		FileInputStream in = null;
		StringBuffer sb = new StringBuffer();
		try {
			in = new FileInputStream(file);
			byte[] b = new byte[2];
			while (in.read(b)!=-1) {
				String string = new String(b);
				sb.append(string);
			}
			//读取文件获取jsonString
			String jsonString = sb.toString().trim();
			String[] split = jsonString.split("\n");
//			System.out.println(split.length);
			for (String string : split) {
				System.out.println(string);
			}
//			System.out.println(jsonString);
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if (in!=null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}*/
		
		/*ArrayList<String> arrayList = new ArrayList<String>();
		arrayList.add("fda");
		arrayList.add("rew");
		System.out.println(arrayList.size());*/
		
		
		
		
		/*String pathname = "d:/bound0727";
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
//		System.out.println(bound);
		HashMap<String, String> map = new HashMap<String, String>();
		String[] records = bound.split("\n");
		for (String record : records) {
			String[] split = record.split("\\s");
			if (split.length == 4) {
				String key = split[0] + "\t" + split[1];
				String value = split[2] + "\t" + split[3];
				map.put(key, value);
			}
		}
		System.out.println(map.get("fda"));*/

		
		/*String tmp = "kinyi--martin--allen--2e-06--100--2e-07";
		String[] split = tmp.split("--");
		for (String string : split) {
			System.out.println(string);
		}*/
		
		File file = new File("d:/test.txt");
		FileInputStream in = null;
		FileOutputStream out = null;
		try {
			in = new FileInputStream(file);
			byte[] b = new byte[100];
			int len;
			while ((len = in.read(b)) != -1) {
				out = new FileOutputStream(new File("d:/aaa.txt"));
				out.write(b, 0, len);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	@org.junit.Test
	public void hello() throws Exception {
		System.out.println("hello");
	}
}
