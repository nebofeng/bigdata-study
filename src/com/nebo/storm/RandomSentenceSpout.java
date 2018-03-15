package com.nebo.storm;
import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
/*
 * 在RandomSentenceSpout中定义了一个字符串数组sentences来模拟数据源。字符串数组中的每句话作为一个tuple发射。
 */
public class RandomSentenceSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;
	SpoutOutputCollector _collector;// 用来向其他Spout发射tuple
	Random _rand;// 用来随机发射一个语句

	// open函数，在ISpout接口中定义，所有的Spout组件在初始化时调用这个方法。在open()中初始化了发射器。
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;// 初始化
		_rand = new Random();// 初始化
	}

	// nextTuple()是所有Spout的核心方法。Storm通过调用这个方法向collector发射tuple。
	@Override
	public void nextTuple() {
		// 每次发射其中一个字符串，阻塞1000ms
		Utils.sleep(1000);
		//定义了待发射的数据源(my love)。Spout从该字符串数组一次取一个字符串生成tuple进行发射。
		String[] sentences = new String[] { 
				"An empty street",
				"An empty house",
				"A hole inside my heart",
				"I'm all alone",
				"The rooms are getting smaller",
				"I wonder how",
				"I wonder why",
				"I wonder where they are",
				"The days we had",
				"The songs we sang together",
				"Oh yeah",
				"And oh my love",
				"I'm holding on forever",
				"Reaching for a love that seems so far",
				"So i say a little prayer",
				"And hope my dreams will take me there",
				"Where the skies are blue to see you once again	, my love",
				"Over seas and coast to coast",
				"To find a place i love the most",
				"Where the fields are green to see you once again ,	my love",
				"I try to read",
				"I go to work",
				"I'm laughing with my friends",
				"But i can't stop to keep myself from thinking",
				"Oh no I wonder how",
				"I wonder why",
				"I wonder where they are",
				"The days we had",
				"The songs we sang together",
				"Oh yeah And oh my love",
				"I'm holding on forever"
				};
		// 生成随机的语句
		String sentence = sentences[_rand.nextInt(sentences.length)];
		// 通过emit方法将构造好的tuple发送出去
		_collector.emit(new Values(sentence));
	}

	//declareOutputFields函数标记了该Spout发射的tuple的(字段值)键值，这里任意取值为sentence
	//游的Bolt可以通过该键值来接收它发出的tuple
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

}
