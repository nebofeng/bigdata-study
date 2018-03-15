package com.nebo.homework.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
 
public class TestPartitioner implements Partitioner  {
   
	public TestPartitioner(VerifiableProperties props) {

   }
	@Override
	public int partition(Object key, int num) {
		int partion = 0; 
		
        partion =  Math.abs(key.hashCode()) % num ;
     
       System.out.println("partion ====>"+partion);
       return partion ;
	}

}
