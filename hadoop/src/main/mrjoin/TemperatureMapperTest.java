import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class TemperatureMapperTest {
	
	private Mapper mapper;
	private MapDriver driver;
	
	
	@Before
	public void init(){
		mapper = new Temperature.TemperatureMapper();
		driver = new MapDriver(mapper);
		
	}
	@Test
	public void test(){
		String line = "2005 01 01 00    22    -6 10117   210    77     6     0     0";
		try {
			driver.withInput(new LongWritable(), new Text(line))
				  .withOutput(new Text("03103"), new IntWritable(22))
				  .runTest();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	

}
