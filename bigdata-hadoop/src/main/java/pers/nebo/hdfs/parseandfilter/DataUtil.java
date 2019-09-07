package pers.nebo.hdfs.parseandfilter;
import java.net.URLDecoder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
/**
 * 
 * 解析机顶盒用户原始数据
 * <GHApp>
 * <WIC cardNum="174041665" stbNum="01050908200014994" 
 * date="2012-09-16" pageWidgetVersion="1.0">
 * <A e="23:56:45" s="23:51:45" n="133" t="2" pi="488" 
 * p="24%E5%B0%8F%E6%97%B6" sn="CCTV-13 新闻" />
 * </WIC>
 * </GHApp>
 *
 */
public class DataUtil {

	@SuppressWarnings("unchecked")
	public static void transData(String text,Context context) {
		try {
			//通过Jsoup解析每行数据
			Document doc = Jsoup.parse(text);
			
			//获取WIC标签内容，每行数据只有一个WIC标签
			Elements content = doc.getElementsByTag("WIC");

			//解析出机顶盒号
			String stbNum = content.get(0).attr("stbNum");
			if(stbNum == null||"".equals(stbNum)){
				return ;
			}
			
			//解析出日期
			String date = content.get(0).attr("date");
			
			if(date == null||"".equals(date)){
				return ;
			}

			//解析A标签
			Elements els = doc.getElementsByTag("A");

			for (Element el : els) {
				//解析结束时间
				String e = el.attr("e");
				if(e ==null||"".equals(e)){
					break;
				}
				//解析起始时间
				String s = el.attr("s");
				if(s == null||"".equals(s)){
					break;
				}

				//解析节目内容
				String p = el.attr("p");
				if(p == null||"".equals(p)){
					break;
				}

				//解析频道
				String sn = el.attr("sn");
				
				if(sn ==null||"".equals(sn)){
					break ;
				}

				//对节目解码
				p = URLDecoder.decode(p, "utf-8");

				//解析出统一的节目名称，比如：天龙八部(1)，天龙八部(2)，同属于一个节目
				int index = p.indexOf("(");

				if (index != -1) {
					p = p.substring(0, index);
				} 

				//起始时间转换为秒
				int startS = TimeUtil.TimeToSecond(s);
				
				//结束时间转换为秒
				int startE = TimeUtil.TimeToSecond(e);

				if (startE < startS) {
					startE = startE + 24 * 3600;
				}
				//每条记录的收看时长
				int duration = startE - startS;

				context.write(new Text(stbNum + "@" + date), new Text(sn + "@" + p+ "@" + s + "@" + e + "@" + duration));

			}
		} catch (Exception e) {
               e.printStackTrace();
		}
	}
}
