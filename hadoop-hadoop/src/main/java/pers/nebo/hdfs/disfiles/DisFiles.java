package pers.nebo.hdfs.disfiles;


import org.apache.hadoop.conf.Configuration;

/**
 * @auther nebofeng
 * @email nebofeng@gmail.com
 * @date 2018/10/31
 * @des MR 处理文件内容分发到多个文件目录
 */
public class DisFiles {
    public static void main(String[] args) {
        //初始化配置
        Configuration conf =new Configuration();
        //可以使用 添加配置文件 conf.addResource();
       // Job
        // 继承 Configured 实现 Tool接口 。

    }

}
