package com.gundan.phoenix.util;

import java.io.UnsupportedEncodingException;
import java.util.Properties;

/**
 * 公共属性读取工具
 * @author liu.jie
 *
 */
public class PropUtils {
	public static final Properties prop = new Properties();
	static {
		try {
			prop.load(PropUtils.class.getResourceAsStream("/prop.properties"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static String getValue(String key) {
		String value = prop.getProperty(key);
		try {
			if(StringUtils.isBlank(value)) {
				return "";
			}
			value = new String(value.getBytes("iso-8859-1"), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return value;
	}
}
