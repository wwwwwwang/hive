package ingeekdata.hiveudf;

import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;

public class HiveUDF extends UDF {
	private final static Pattern PATTERN_COLON = Pattern.compile(":");
	private final static Pattern PATTERN_SPACE = Pattern.compile(" ");

	/*
	 * public static String format(String timestr) throws ParseException{ String
	 * str = ""; SimpleDateFormat format = new
	 * SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); Date date =
	 * format.parse(timestr); str = date.toString();
	 * System.out.println("time string = " + date); return str; }
	 */
	// timestr = 2016-10-20 09:42:07
	public String evaluate(String timestr) {
		return evaluate(timestr, 5);
	}

	public String evaluate(String timestr, int interval) {
		String res = "";
		int min = 0;
		boolean iserr = false;
		try {
			if (timestr.trim().contains(" ")) {
				String[] datetime = PATTERN_SPACE.split(timestr);
				res += datetime[0];
				String[] hms = PATTERN_COLON.split(datetime[1].trim());
				res += " " + hms[0] + ":";
				min = Integer.parseInt(hms[1]);
			} else {
				String[] hms = PATTERN_COLON.split(timestr.trim());
				res += hms[0] + ":";
				min = Integer.parseInt(hms[1]);
			}
		} catch (Exception e) {
			iserr = true;
		}
		if (iserr == true) {
			return timestr;
		} else {
			min = min - min % interval;
			res += min + ":00";
			return res;
		}
	}

	// 测试的main方法
	public static void main(String[] args) throws Exception {
		HiveUDF hudf = new HiveUDF();
		String date_str = "2015-01-01 19:33:0";
		String date_str1 = "2015-01-01 19:38:00";
		String date_str2 = "2015-01-01 19:34:59";
		String date_str3 = "2015-01-01 19:35:00";
		String date_str4 = "2015-01-01 19:37:45";
		String date_str5 = " 19:37:45";
		String date_str6 = "19:41:09";
		String date_str7 = "2015-01-01 19:37";
		String date_str8 = "2015-01-0119:37:45";
		String date_str9 = "19:41_09";
		System.out.println(hudf.evaluate(date_str));
		System.out.println(hudf.evaluate(date_str1));
		System.out.println(hudf.evaluate(date_str2));
		System.out.println(hudf.evaluate(date_str3));
		System.out.println(hudf.evaluate(date_str4));
		System.out.println(hudf.evaluate(date_str3, 10));
		System.out.println(hudf.evaluate(date_str4, 10));
		System.out.println(hudf.evaluate(date_str5));
		System.out.println(hudf.evaluate(date_str6));
		System.out.println(hudf.evaluate(date_str5, 20));
		System.out.println(hudf.evaluate(date_str6, 20));
		System.out.println(hudf.evaluate(date_str7));
		System.out.println(hudf.evaluate(date_str8));
		System.out.println(hudf.evaluate(date_str9));
	}
}
