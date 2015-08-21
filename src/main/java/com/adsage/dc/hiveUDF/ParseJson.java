package com.adsage.dc.hiveUDF;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.adsage.dc.multiCount_github.JsonUtils;

public class ParseJson extends UDF{

	public String evaluate(String jsonString){
		String json = JsonUtils.parseOneJsonObj(jsonString, "content_type");
		return json;
	}
	
}

