package pers.nebo.mr.searchhot;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapReduceSearchHot extends Configured implements Tool {
	public static class ActorMapper extends Mapper< Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//value=name+gender+hotIndex
			String[] tokens = value.toString().split("\t");
			String gender = tokens[1].trim();//性别
			String nameHotIndex = tokens[0] + "\t" + tokens[2];//名称和搜索指数
			context.write(new Text(gender), new Text(nameHotIndex));
 		}
	}
	
	public static class ActorCombiner extends Reducer< Text, Text, Text, Text> {
		private Text text = new Text();
		@Override
		public void reduce(Text key, Iterable< Text> values, Context context) throws IOException, InterruptedException {
			int maxHotIndex = Integer.MIN_VALUE;
			int hotIndex = 0;
			String name="";
			for (Text val : values) {
				String[] valTokens = val.toString().split("\\t");
				hotIndex = Integer.parseInt(valTokens[1]);
				if(hotIndex>maxHotIndex){
					name = valTokens[0];
					maxHotIndex = hotIndex;
				}
			}
			text.set(name+"\t"+maxHotIndex);
			context.write(key, text);	
		}
	}
	
	public static class ActorPartitioner extends Partitioner< Text, Text> {		 
		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) { 
			String sex = key.toString();           
			if(numReduceTasks==0)
				return 0;
			//性别为male 选择分区0
			if(sex.equals("male"))             
				return 0;
			//性别为female 选择分区1
			if(sex.equals("female"))
				return 1 % numReduceTasks;
			//其他性别 选择分区2
			else
				return 2 % numReduceTasks;
		}	 
	}
	public static class ActorReducer extends Reducer< Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable< Text> values, Context context) throws IOException, InterruptedException {
			int maxHotIndex = Integer.MIN_VALUE;
			String name = " ";
			int hotIndex = 0;
			for (Text val : values) {
				String[] valTokens = val.toString().split("\\t");
				hotIndex = Integer.parseInt(valTokens[1]);
				if (hotIndex > maxHotIndex) {
					name = valTokens[0];
					maxHotIndex = hotIndex;
				}
			}
			context.write(new Text(name), new Text( key + "\t"+ maxHotIndex));
		}
	} 
		  

	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();//读取配置文件
		
		Path mypath = new Path(args[1]);
		FileSystem hdfs = mypath.getFileSystem(conf);
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}

		Job job = new Job(conf, "MapReduceSearchHot");//新建一个任务
		job.setJarByClass(MapReduceSearchHot.class);//主类
		
		job.setNumReduceTasks(2);//reduce的个数设置为2
		job.setPartitionerClass(ActorPartitioner.class);//设置Partitioner类
					
		job.setMapperClass(ActorMapper.class);//Mapper
		job.setMapOutputKeyClass(Text.class);//map 输出key类型
		job.setMapOutputValueClass(Text.class);//map 输出value类型
				
		job.setCombinerClass(ActorCombiner.class);//设置Combiner类
		
		job.setReducerClass(ActorReducer.class);//Reducer
		job.setOutputKeyClass(Text.class);//输出结果 key类型
		job.setOutputValueClass(Text.class);//输出结果 value类型
		
		FileInputFormat.addInputPath(job, new Path(args[0]));// 输入路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));// 输出路径
		job.waitForCompletion(true);//提交任务
		return 0;
	}
	
	
	/**
	 * @function main 方法
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		String[] args0 = { "hdfs://nebo:9000/middle/actor/actor.txt",
				"hdfs://nebo:9000/middle/actor/out/" };
		int ec = ToolRunner.run(new Configuration(), new MapReduceSearchHot(), args0);
		System.exit(ec);
	}
}
