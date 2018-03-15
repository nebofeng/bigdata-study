package com.nebo.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
/**
 * SplitSentenceBlot接收RandomSentenceSpout发射的tuple，
 * 它将每句话分割成每个单词，并将每个单词作为tuple发射。
 */
public class SplitSentenceBlot extends BaseBasicBolt {
    
	private static final long serialVersionUID = 1L;

	public void execute(Tuple input, BasicOutputCollector collector) {
        	//通过sentence字段接收从RandomSentenceSpout的发射器发射过来的tuple       	
            String sentence  = input.getStringByField("sentence");
            //将接受过来的句子sentence，解析为单词数组
            String[] words = sentence.split("\\s+");
            for(String word : words){
            	//将每个单词构造成tuple并发送给下一个Spout
                collector.emit(new Values(word));
            }
    }

	//定义SplitSentenceBolt发送的tuple的字段("键值")为 word
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
