package com.wujianbo.udtf;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;

import java.util.HashMap;
import java.util.Map;

/**
 * 给定指定规则的key:value字符串,解析其中对应的key值
 */
@Resolve({"string,string,string,string->string"})
public class KeyValue extends UDTF {
	@Override
	public void process(Object[] args) throws UDFException{
		String source = (String) args[0];
		String pareSeparator = (String) args[1];
		String keySeparator = (String) args[2];
		String key = (String) args[3];
		String[] keyValueArray =  source.split(pareSeparator);
		Map<String,String> keyValueMap = new HashMap<>();
		if(keyValueArray.length > 0){
			for(String item : keyValueArray){
				String[] keyValueItemArray = item.split(keySeparator);
				if(keyValueArray.length == 2){
					keyValueMap.put(keyValueItemArray[0],keyValueItemArray[1]);
				}
			}
		}
		if(keyValueMap.containsKey(key)){
			forward(keyValueMap.get(key));
		}
	}
}
