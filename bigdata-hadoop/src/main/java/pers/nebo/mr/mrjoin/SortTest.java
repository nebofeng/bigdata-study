package main.mrjoin;

import main.mrjoin.Anagram.AnagramMapper;
import main.mrjoin.Anagram.AnagramReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class SortTest {
	
	AnagramMapper mapper = new AnagramMapper();
	AnagramReducer reducer = new AnagramReducer();
	private ReduceDriver driver;
	@Test
	public void sortTest(){
//		String result = mapper.Sort("      aghdsaf");
//		System.out.println(result);
		
		String key = "abcd";
		List values = new ArrayList();
		values.add(new Text("abcd"));
		values.add(new Text("acbd"));
		 
		driver =   new ReduceDriver(reducer);
		try {
			driver.withInput(new Text(key), values)
			       .withOutput(new Text(key), new Text("abcd,acbd"))
			       .runTest();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
