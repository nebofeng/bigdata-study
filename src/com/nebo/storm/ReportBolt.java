package com.nebo.storm;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
/**
 * 
 * ReportBolt接收WordCountBolt发送的tuple，
 * 将统计的结果存入HashMap中，并打印出结果。
 *
 */
public class ReportBolt extends BaseRichBolt{
    private static final long serialVersionUID = 4921144902730095910L;
    //定义HashMap存储单词统计个数
    private HashMap counts = null;
    
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        // TODO Auto-generated method stub
    	//初始化HashMap
        this.counts = new HashMap();
    }

    public void execute(Tuple input) {
        // TODO Auto-generated method stub
    	//获取单词名称
        String word = input.getStringByField("word");
        //获取单词个数
        Integer count = input.getIntegerByField("count");
        //使用HashMap存储单词和单词数
        this.counts.put(word, count);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
        //不需要发出任何数据流
    }
    
    //Topology在storm集群中运行时，cleanup方法是不可靠的,并不能保证它一定会执行
    public void cleanup(){
        System.out.println("------ print counts for my love------");
        List<String> keys = new ArrayList<String>();
        //将HashMap中所有的单词都添加到一个集合里
        keys.addAll(counts.keySet());
        //对键(单词)进行排序
        Collections.sort(keys);
        //输出排好序的每个单词的出现次数
        for(String key : keys)
            System.out.println(key + " : " + this.counts.get(key));
    }
}