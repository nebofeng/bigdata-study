package com.nebo.storm;

import java.util.HashMap;
import java.util.Map;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
/**
 * 
 * WordCountBolt接收SplitSentenceBlot发送的tuple，它将接收到的每一个单词统计计数
 * 并将 <单词：出现次数> 作为tuple发射
 *
 */
public  class WordCountBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;
	//统计每个单词出现的次数，放到HashMap中保存起来
	private Map counts = new HashMap();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      //接收从SplitSentenceBlot的发射器发射过来的tuple
      String word = tuple.getStringByField("word");
      Integer count = (Integer) counts.get(word);
      //如果HashMap中没有word这个单词
      if (count == null)
        count = 0;
      count++;
      //更新该单词在HashMap中的统计次数
      counts.put(word, count);
      System.out.println(word+","+count);
      //第一个元素的键为 "word"，值为该单词(a string)，第二个键为 "count",值为单词的计数
      collector.emit(new Values(word, count));
    }

    //定义WordCountBolt发送的tuple的字段为 word和count
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }
