package com.nebo.mergefiles;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @auther nebofeng
 * @email nebofeng@gmail.com
 * @date 2018/10/31
 * @des :  合并本地小文件到hdfs
 */
public class MergeFiles {
    //hdfs文件系统
    private static FileSystem fs = null;
    //返回本地hdfs
    private static FileSystem local = null;
    public static void uploadAndMergeList( Path srcPath ,Path dstPath) throws IOException, URISyntaxException {
        Configuration conf =new Configuration();
        URI uri = new URI("hdfs://nebo:9000");

        fs = FileSystem.get(uri, conf);
        local =fs.getLocal(conf);

        FileStatus[] dfileStatus = local.globStatus(srcPath, new RegexExcludePathFilter("^.*vpn$"));
        Path[] dirList = FileUtil.stat2Paths(dfileStatus);

        FSDataOutputStream out = null;
        for(Path p : dirList){
            String namePath = p.toString();
            int i = namePath.lastIndexOf("/");
            String uploadName = namePath.substring(i+1, namePath.length());
            out = fs.create(new Path(dstPath+"/"+uploadName+".txt"));
            FileStatus[] ffileStatus=local.globStatus(new Path(p+"/*"), new RegexAcceptPathFilter("^.*txt$"));
            Path[] file = FileUtil.stat2Paths(ffileStatus);
            FSDataInputStream in = null;
            for(Path pf: file){
                in = local.open(pf);
                IOUtils.copyBytes(in, out, 4096, false) ;
            }

        }
        fs.close();

    }
}
