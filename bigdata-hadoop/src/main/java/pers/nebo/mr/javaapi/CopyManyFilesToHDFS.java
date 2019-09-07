package pers.nebo.mr.javaapi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class CopyManyFilesToHDFS {

    private static FileSystem fs = null;
    private static FileSystem local = null;

    public static void main(String[] args) throws IOException, URISyntaxException {
        uploadList(new Path("D://Document/Bigdata/205/205_data/data/*"), new Path("hdfs://nebo:9000/nebo1/upload/"));


    }


    public static void uploadList(Path srcPath, Path dstPath) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();

        URI uri = new URI("hdfs://nebo:9000");

        fs = FileSystem.get(uri, conf);

        local = fs.getLocal(conf);

        FileStatus[] fileStatus = local.globStatus(srcPath, new RegexAcceptPathFilter("^.*txt$"));

        Path[] listedPaths = FileUtil.stat2Paths(fileStatus);

        for (Path p : listedPaths) {
            fs.copyFromLocalFile(p, dstPath);
        }


    }

}
