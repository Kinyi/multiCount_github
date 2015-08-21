package com.adsage.dc.multiCount_github;

import java.util.HashMap;
import java.util.Map.Entry;

import org.json.JSONObject;

public class JsonUtils {

	/**
	 * 解析非嵌套JSON对象的属性
	 * 
	 * @param content
	 * @param Attr
	 * @return
	 */
	public static String parseOneJsonObj(String content, String Attr) {
		String result = null;
		JSONObject jsonObject = new JSONObject(content);
		result = jsonObject.getString(Attr);
		return result;
	}

	/**
	 * 解析一层嵌套的JSON对象的属性
	 * 
	 * @param content
	 * @param firstAttr
	 * @param secondAttr
	 * @return
	 */
	public static String parseTwoJsonObj(String content, String firstAttr,
			String secondAttr) {
		String result = null;
		JSONObject jsonObject = new JSONObject(content);
		JSONObject nestedJsonObject = jsonObject.getJSONObject(firstAttr);
		result = nestedJsonObject.getString(secondAttr);
		return result;
	}
	
	/**
	 * 解析两层嵌套的JSON对象的属性
	 * 
	 * @param content
	 * @param firstAttr
	 * @param secondAttr
	 * @return
	 */
	public static long parseThreeJsonObj(String content, String firstAttr,
			String secondAttr, String thirdAttr) {
		long result = 0L;
		JSONObject jsonObject = new JSONObject(content);
		JSONObject firstNestedJsonObject = jsonObject.getJSONObject(firstAttr);
		JSONObject secondNestedJsonObject = firstNestedJsonObject.getJSONObject(secondAttr);
		result = secondNestedJsonObject.getLong(thirdAttr);
		return result;
	}

	/**
	 * 解析一层嵌套但不知嵌套JSON对象的属性值
	 * 
	 * @param content
	 * @param attr
	 * @return
	 */
	public static String parseTwoNoNestedAttr(String content, String attr) {
		String result = "";
		JSONObject jsonObject = new JSONObject(content);
		JSONObject nestedJsonObj = jsonObject.getJSONObject(attr);
		for (Object key : nestedJsonObj.keySet()) {
			if (key instanceof String) {
				String k = (String) key;
				String value = nestedJsonObj.getString(k);
				result += k + "\t" + value + ",";
			}
		}
		return result;
	}

	/**
	 * 生成一个一层嵌套的JSON对象
	 * 
	 * @param attr
	 * @param keyvalue
	 * @return
	 */
	public static String generateJson(String attr, HashMap<String, String> keyvalue) {
		JSONObject childObj = new JSONObject();
		for (Entry<String, String> map : keyvalue.entrySet()) {
			String key = map.getKey();
			String value = map.getValue();
			childObj.put(key, value);
		}
		JSONObject parentObj = new JSONObject();
		parentObj.put(attr, childObj);
		String result = parentObj.toString();
		return result;
	}
	
	/**
	 * 生成一个两层嵌套的JSON对象
	 * {outerAttr:{innerAttr:{key:value,key:value}}}
	 * @param attr
	 * @param keyvalue
	 * @return
	 */
	public static String generateTwoJson(String outerAttr, String innerAttr, HashMap<String, String> keyvalue) {
		JSONObject childObj = new JSONObject();
		for (Entry<String, String> map : keyvalue.entrySet()) {
			String key = map.getKey();
			String value = map.getValue();
			childObj.put(key, value);
		}
		JSONObject secondParentObj = new JSONObject();
		secondParentObj.put(innerAttr, childObj);
		JSONObject firstParentObj = new JSONObject();
		firstParentObj.put(outerAttr, secondParentObj);
		String result = firstParentObj.toString();
		return result;
	}
	
	/*public static String generateNestedJson(String attr, JSONObject jsonObj){
		
		return null;
	}*/
}

