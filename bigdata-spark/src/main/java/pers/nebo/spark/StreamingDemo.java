package pers.nebo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
 import scala.Tuple2;

import java.util.Arrays;

/**
 * @auther nebofeng@gmail.com
 * @date 2018/9/25 21:28
 */
public class StreamingDemo {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("StreamingDemo").setMaster("local");
		//JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(10));

		JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream("localhost", 8088);
		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
		JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);

// Print the first ten elements of each RDD generated in this DStream to the console
		wordCounts.print();
		streamingContext.start();
		try {
			streamingContext.awaitTermination();
		} catch (Exception e) {
			e.printStackTrace();
		}

		//basic Sources
		//streamingContext.fileStream("");


	}
}
