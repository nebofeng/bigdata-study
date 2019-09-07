package pers.nebo.hdfs.parseandfilter;

import java.util.ArrayList;
import java.util.List;
/**
 * 
 * 时间工具
 *
 */
public class TimeUtil {
	/**
	 * 将时间00:00:00转换为秒 int
	 * 
	 * @param time
	 * @return
	 */
	public static int TimeToSecond(String time) {
		if (time == null||time.equals("")) {
			return 0;
		}
		String[] my = time.split(":");
		int hour = Integer.parseInt(my[0]);
		int min = Integer.parseInt(my[1]);
		int sec = Integer.parseInt(my[2]);
		int totalSec = hour * 3600 + min * 60 + sec;

		return totalSec;
	}

	/**
	 * 将时间00:00:00转换为秒 String
	 * 
	 * @param time
	 * @return
	 */
	public static String TimeToSecond2(String time) {
		if (time == null) {
			return "";
		}
		String[] my = time.split(":");
		int hour = Integer.parseInt(my[0]);
		int min = Integer.parseInt(my[1]);
		int sec = Integer.parseInt(my[2]);
		int totalSec = hour * 3600 + min * 60 + sec;

		return totalSec + "";
	}

	/**
	 * 求两个时间的字符串差值
	 * @param a_e
	 * @param a_s
	 * @return
	 */
	public static String getDuration(String a_e, String a_s) {
		if (a_e == null || a_s == null) {
			return 0 + "";
		}
		int ae = Integer.parseInt(a_e);
		int as = Integer.parseInt(a_s);
		return (ae - as) + "";

	}

	/**
	 * 将时间 00:00转换为秒 int
	 * 
	 * @param time
	 * @return
	 */
	public static int Time2ToSecond(String time) {
		if (time == null) {
			return 0;
		}
		String[] my = time.split(":");
		int hour = Integer.parseInt(my[0]);
		int min = Integer.parseInt(my[1]);
		int totalSec = hour * 3600 + min * 60;

		return totalSec;
	}

	/**
	 * 提取start end 之间的分钟数
	 * 
	 * @param time
	 * @return
	 */
	public static List<String> getTimeSplit(String start, String end) {
		List<String> list = new ArrayList<String>();
		String[] s = start.split(":");
		int sh = Integer.parseInt(s[0]);
		int sm = Integer.parseInt(s[1]);
		String[] e = end.split(":");
		int eh = Integer.parseInt(e[0]);
		int em = Integer.parseInt(e[1]);
		if (eh < sh) {
			eh = 24;
		}
		if (sh == eh) {
			for (int m = sm; m <= em; m++) {
				int am = m + 1;
				int ah = sh;
				if (am == 60) {
					am = 0;
					ah += 1;
				}
				String hstr = "";
				String mstr = "";
				if (sh < 10) {
					hstr = "0" + sh;
				} else {
					hstr = sh + "";
				}
				if (m < 10) {
					mstr = "0" + m;
				} else {
					mstr = m + "";
				}
				String time =  hstr + ":" + mstr ;
				list.add(time);
			}
		} else {
			for (int h = sh; h <= eh; h++) {
				if (h == 24) {
					break;
				}
				if (h == sh) {
					for (int m = sm; m <= 59; m++) {
						int am = m + 1;
						int ah = h;
						if (am == 60) {
							am = 0;
							ah += 1;
						}
						String hstr = "";
						String mstr = "";
						if (h < 10) {
							hstr = "0" + h;
						} else {
							hstr = h + "";
						}
						if (m < 10) {
							mstr = "0" + m;
						} else {
							mstr = m + "";
						}
						String time =  hstr + ":" + mstr ;
						list.add(time);
					}
				} else if (h == eh) {
					for (int m = 0; m <= em; m++) {
						int am = m + 1;
						int ah = h;
						if (am == 60) {
							am = 0;
							ah += 1;
						}
						String hstr = "";
						String mstr = "";
						if (h < 10) {
							hstr = "0" + h;
						} else {
							hstr = h + "";
						}
						if (m < 10) {
							mstr = "0" + m;
						} else {
							mstr = m + "";
						}
						String time = hstr + ":" + mstr ;
						list.add(time);
					}
				} else {
					for (int m = 0; m <= 59; m++) {
						int am = m + 1;
						int ah = h;
						if (am == 60) {
							am = 0;
							ah += 1;
						}
						String hstr = "";
						String mstr = "";
						if (h < 10) {
							hstr = "0" + h;
						} else {
							hstr = h + "";
						}
						if (m < 10) {
							mstr = "0" + m;
						} else {
							mstr = m + "";
						}
						String time = hstr + ":" + mstr ;
						list.add(time);
					}
				}
			}
		}
		return list;
	}
}
