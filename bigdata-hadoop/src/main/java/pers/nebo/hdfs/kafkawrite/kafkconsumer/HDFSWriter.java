package pers.nebo.hdfs.kafkawrite.kafkconsumer;

import org.apache.hadoop.fs.FSDataOutputStream;

/**
 *hdfs写入器
 */
public class HDFSWriter {

    /**
     * 写入log到hdfs文件
     * hdfs://mycluster/eshop/2017/02/28/s201.log | s202.log | s203.log
     */
    public void writeLog2HDFS(String path, byte[] log) {
        try {
            //得到我们的装饰流
            FSDataOutputStream out = HDFSOutputStreamPool.getInstance().takeOutputStream(path);
            out.write(log);
            out.write("\r\n".getBytes());
            out.hsync();
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
