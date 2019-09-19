package pers.nebo.mr.mrjoin;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TemperatureTest {
	private Mapper mapper;
	private Reducer reducer;
	private MapReduceDriver driver;
	
	
	@Before
	public void init(){
		mapper = new Temperature.TemperatureMapper();
		reducer = new Temperature.TemperatureReducer();
		driver = new MapReduceDriver(mapper ,reducer);		
	}
	@Test
	public void test(){
		String line = "2005 01 01 00    22    -6 10117   210    77     6     0     0";
		String line1 = "2005 01 01 00    23    -6 10117   210    77     6     0     0";
		try {
			driver.withInput(new LongWritable(), new Text(line))
			      .withInput(new LongWritable(), new Text(line1))
			       
					.withOutput(new Text("03103"), new FloatWritable((float) 22.5))
					.runTest();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	
}
