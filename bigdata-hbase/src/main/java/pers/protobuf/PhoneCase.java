package pers.protobuf;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import pers.protobuf.Phone.PhoneDetail;

import pers.protobuf.Phone.DayOfPhone;


import pers.protobuf.Phone;

public class PhoneCase {

	// 表的管理类
	HBaseAdmin admin = null;
	// 数据的管理类
	HTable table = null;
	// 表名
	String tm = "phone";

	/**
	 * 完成初始化功能
	 *
	 * @throws Exception
	 */
	@Before
	public void init() throws Exception {
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "node1,node2,node3");
		admin = new HBaseAdmin(conf);
		table = new HTable(conf, tm.getBytes());
	}

	/**
	 * 创建表
	 *
	 * @throws Exception
	 */
	@Test
	public void createTable() throws Exception {
		// 表的描述类
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tm));
		// 列族的描述类
		HColumnDescriptor family = new HColumnDescriptor("cf".getBytes());
		desc.addFamily(family);
		if (admin.tableExists(tm)) {
			admin.disableTable(tm);
			admin.deleteTable(tm);
		}
		admin.createTable(desc);
	}

	/**
	 * 10个用户，每个用户每年产生1000条通话记录
	 *
	 * dnum:对方手机号 type:类型：0主叫，1被叫 length：长度 date:时间
	 *
	 * @throws Exception
	 *
	 */
	@Test
	public void insert() throws Exception {
		List<Put> puts = new ArrayList<Put>();
		for (int i = 0; i < 10; i++) {
			String phoneNumber = getPhone("158");
			for (int j = 0; j < 1000; j++) {
				// 属性
				String dnum = getPhone("177");
				String length = String.valueOf(r.nextInt(99));
				String type = String.valueOf(r.nextInt(2));
				String date = getDate("2018");
				// rowkey设计
				String rowkey = phoneNumber + "_" + (Long.MAX_VALUE - sdf.parse(date).getTime());
				Put put = new Put(rowkey.getBytes());
				put.add("cf".getBytes(), "dnum".getBytes(), dnum.getBytes());
				put.add("cf".getBytes(), "length".getBytes(), length.getBytes());
				put.add("cf".getBytes(), "type".getBytes(), type.getBytes());
				put.add("cf".getBytes(), "date".getBytes(), date.getBytes());
				puts.add(put);
			}
		}
		table.put(puts);
	}

	/**
	 * 查询某一个用户3月份的所有通话记录 条件： 1、某一个用户 2、时间
	 *
	 * @throws Exception
	 */
	@Test
	public void scan() throws Exception {
		String phoneNumber = "15895223166";
		String startRow = phoneNumber + "_" + (Long.MAX_VALUE - sdf.parse("20180401000000").getTime());
		String stopRow = phoneNumber + "_" + (Long.MAX_VALUE - sdf.parse("20180301000000").getTime());

		Scan scan = new Scan();
		scan.setStartRow(startRow.getBytes());
		scan.setStopRow(stopRow.getBytes());
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.print(Bytes
					.toString(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "dnum".getBytes()))));
			System.out.print("--" + Bytes
					.toString(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "type".getBytes()))));
			System.out.print("--" + Bytes
					.toString(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "date".getBytes()))));
			System.out.println("--" + Bytes
					.toString(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "length".getBytes()))));
		}
	}

	/**
	 * 查询某一个用户。所有的主叫电话 条件： 1、电话号码 2、type=0
	 *
	 * @throws Exception
	 */
	@Test
	public void scan2() throws Exception {
		FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter("cf".getBytes(), "type".getBytes(),
				CompareOp.EQUAL, "0".getBytes());
		PrefixFilter filter2 = new PrefixFilter("15895223166".getBytes());
		filters.addFilter(filter1);
		filters.addFilter(filter2);

		Scan scan = new Scan();
		scan.setFilter(filters);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.print(Bytes
					.toString(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "dnum".getBytes()))));
			System.out.print("--" + Bytes
					.toString(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "type".getBytes()))));
			System.out.print("--" + Bytes
					.toString(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "date".getBytes()))));
			System.out.println("--" + Bytes
					.toString(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "length".getBytes()))));
		}
		scanner.close();
	}

	/**
	 *
	 * @param string
	 * @return
	 */

	private String getDate(String string) {
		return string + String.format("%02d%02d%02d%02d%02d", r.nextInt(12) + 1, r.nextInt(31), r.nextInt(24),
				r.nextInt(60), r.nextInt(60));
	}

	Random r = new Random();
	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");

	private String getPhone(String phonePrefix) {
		return phonePrefix + String.format("%08d", r.nextInt(99999999));
	}

	/**
	 * 10个用户，每个用户1000条，每一条记录当作一个对象进行存储
	 * @throws Exception
	 */
	@Test
	public void insert2() throws Exception{
		List<Put> puts = new ArrayList<Put>();
		for(int i = 0;i<10;i++){
			String phoneNumber = getPhone("158");
			for(int j = 0;j<1000;j++){
				String dnum = getPhone("177");
				String length =String.valueOf(r.nextInt(99));
				String type = String.valueOf(r.nextInt(2));
				String date = getDate("2018");
				//保存属性到对象中
				Phone.PhoneDetail.Builder phoneDetail = Phone.PhoneDetail.newBuilder();
				phoneDetail.setDate(date);
				phoneDetail.setLength(length);
				phoneDetail.setType(type);
				phoneDetail.setDnum(dnum);

				//rowkey
				String rowkey = phoneNumber+"_"+(Long.MAX_VALUE-sdf.parse(date).getTime());

				Put put = new Put(rowkey.getBytes());
				put.add("cf".getBytes(), "phoneDetail".getBytes(), phoneDetail.build().toByteArray());
				puts.add(put);
			}
		}
		table.put(puts);
	}

	@Test
	public void get() throws Exception{
		Get get = new Get("15866626435_9223370522183722807".getBytes());
		Result result = table.get(get);
		PhoneDetail phoneDetail = Phone.PhoneDetail.parseFrom(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "phoneDetail".getBytes())));
		System.out.println(phoneDetail);
	}

	/**
	 * 10个用户，每天产生1000条记录，每一天的所有数据放到一个rowkey中
	 * @throws Exception
	 */
	@Test
	public void insert3() throws Exception{
		List<Put> puts = new ArrayList<Put>();

		for(int i = 0;i<10;i++){
			String phoneNumber = getPhone("133");
			String rowkey = phoneNumber+"_"+(Long.MAX_VALUE-sdf.parse("20181225000000").getTime());
			Phone.DayOfPhone.Builder dayOfPhone = Phone.DayOfPhone.newBuilder();
			for(int j=0;j<1000;j++){
				String dnum = getPhone("177");
				String length =String.valueOf(r.nextInt(99));
				String type = String.valueOf(r.nextInt(2));
				String date = getDate2("20181225");

				Phone.PhoneDetail.Builder phoneDetail = Phone.PhoneDetail.newBuilder();
				phoneDetail.setDate(date);
				phoneDetail.setLength(length);
				phoneDetail.setType(type);
				phoneDetail.setDnum(dnum);
				dayOfPhone.addDayPhone(phoneDetail);
			}
			Put put = new Put(rowkey.getBytes());
			put.add("cf".getBytes(), "day".getBytes(), dayOfPhone.build().toByteArray());
			puts.add(put);
		}
		table.put(puts);

	}

	@Test
	public void get2() throws Exception{
		Get get = new Get("13398049199_9223370491187575807".getBytes());
		Result result = table.get(get);
		DayOfPhone parseFrom = Phone.DayOfPhone.parseFrom(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "day".getBytes())));
		int count = 0;
		for (PhoneDetail pd : parseFrom.getDayPhoneList()) {
			System.out.println(pd);
			count++;
		}
		System.out.println(count);
	}

	private String getDate2(String string) {
		return string+String.format("%02d%02d%02d", r.nextInt(24),r.nextInt(60),r.nextInt(60));
	}

	@After
	public void destory() throws Exception {

		if (admin != null) {
			admin.close();
		}
	}
}
