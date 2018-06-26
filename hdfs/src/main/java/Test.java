import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class Test {
    private static FileSystem fs =null;
    private static FileSystem local = null;
    public static void main(String[] args)  throws IOException, URISyntaxException {
        Configuration conf =new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        URI uri = new URI("hdfs://139.199.172.112:9000");

        fs = FileSystem.get(uri, conf);

        local =fs.getLocal(conf);




        //本地文件

        Path src =new Path("D:\\test");

        //HDFS

        Path dst =new Path("hdfs://139.199.172.112:9000/home/");



        fs.copyFromLocalFile(src, dst);

        System.out.println("Upload to"+conf.get("fs.default.name"));





    }
}
