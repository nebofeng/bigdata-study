package pers.nebo.hdfs.parseandfilter;

public class StringUtils {

	/*
	 * 将字符串转换为数组
	 */
	public static String[] split(String value,String regex){
		if(value==null)
			value = "";
		String[] valueItems = value.split(regex);
		return valueItems;
	}
}
