package pers.nebo.mr.mrjoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TemperatureReducerTest {
	
	private Reducer reducer;
	private ReduceDriver driver;
	@Before
	public void init(){
		reducer = new Temperature.TemperatureReducer();
		driver = new ReduceDriver(reducer);
	}
	
	
	@Test
	public void test(){
		String key = "03103";
		List values = new ArrayList();
		values.add(new IntWritable(200));
		values.add(new IntWritable(100));
		values.add(new IntWritable(300));
		values.add(new IntWritable(400));
		
		try {
			driver.withInput(new Text(key), values)
			       .withOutput(new Text(key), new IntWritable(250))
			       .runTest();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	

}
