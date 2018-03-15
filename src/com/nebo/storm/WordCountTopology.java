package com.nebo.storm;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
/**
 * 
 * 构建Wordcount Topology
 *
 */
public class WordCountTopology {
	private static final String SENTENCE_SPOUT_ID = "sentence-spout";
	private static final String SPLIT_BOLT_ID = "split-bolt";
	private static final String COUNT_BOLT_ID = "count-bolt";
	private static final String REPORT_BOLT_ID = "report-bolt";
	private static final String TOPOLOGY_NAME = "wordcount-topology";

	public static void main(String[] args) throws Exception {

		// 构造一个RandomSentenceSpout对象
		RandomSentenceSpout sentenceSpout = new RandomSentenceSpout();
		// 构造一个SplitSentenceBlot对象
		SplitSentenceBlot splitBolt = new SplitSentenceBlot();
		// 构造一个WordCountBolt对象
		WordCountBolt countBolt = new WordCountBolt();
		// 构造一个ReportBolt对象
		ReportBolt reportBolt = new ReportBolt();

		TopologyBuilder builder = new TopologyBuilder();

		// 设置sentenceSpout，并行度为5
		builder.setSpout(SENTENCE_SPOUT_ID, sentenceSpout, 5);

		// 设置splitBolt，并行度为8
		builder.setBolt(SPLIT_BOLT_ID, splitBolt, 8).shuffleGrouping(
				SENTENCE_SPOUT_ID);

		// 设置countBolt，并行度为12
		builder.setBolt(COUNT_BOLT_ID, countBolt, 12).fieldsGrouping(
				SPLIT_BOLT_ID, new Fields("word"));

		// 设置reportBolt
		builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(
				COUNT_BOLT_ID);

		Config conf = new Config();
		conf.setDebug(true);

		if (args != null && args.length > 0) {
			// 提交集群运行
			conf.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
					builder.createTopology());
		} else {
			// 本地测试运行
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(TOPOLOGY_NAME, conf,
					builder.createTopology());
			Thread.sleep(10000);
			cluster.shutdown();
		}
	}
}