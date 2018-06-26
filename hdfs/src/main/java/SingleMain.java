import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 单个目录多个文件上传
 */
public class SingleMain {


    static  Path uploadPath = new Path("hdfs://139.199.172.112:9000/home");
    Path sourcePath = new Path("E:\\CodeDocument\\20180626test");
   static  Configuration conf =new Configuration();
   static  URI uri;
    static   FileSystem fs;
    static {
        try {

             conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

            uri = new URI("hdfs://139.199.172.112:9000");
           fs  = FileSystem.get(uri, conf);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public static void main(String[] args)  throws IOException, URISyntaxException {

        String filePath = "E:\\CodeDocument\\20180626test";
        List<File>  fileArrayList = new ArrayList<File>();
        int  coreSize = 5;
        ExecutorService exe = Executors.newFixedThreadPool(coreSize);
        fileArrayList = SingleGetFilList.getFileList(filePath);
        if(fileArrayList.size()<coreSize){
            coreSize=fileArrayList.size();
        }
        int threadDealSize= fileArrayList.size() / coreSize ;//不会等于零 。文件数目不会小于 。
        int  moreSize    = fileArrayList.size() % coreSize; //剩余不够平均分配的条件数据。

        int start = 0;

        for(int i=0;i<moreSize;i++){

            MyThread newThread = new MyThread(fileArrayList.subList(start,start+threadDealSize+1));
            newThread.start();
         //   exe.submit(new MyThread(fileArrayList.subList(start,start+threadDealSize+1)));
            start = start+threadDealSize+1;
        }
        for(int i=0;i<coreSize-moreSize;i++){
            MyThread newThread = new MyThread(fileArrayList.subList(start,start+threadDealSize));
            newThread.start();
          //  exe.submit(new MyThread(fileArrayList.subList(start,start+threadDealSize)));
            start= start+threadDealSize;
        }

    }


    public static  void CreateFile(String dst ,String src) throws IOException {

        Path dstPath = new Path(dst);//目标路径
        FSDataOutputStream outputStream = fs.create(dstPath);

        FileInputStream fis = null;
        fis = new FileInputStream(src);

        byte[] buff = new byte[1024];
        int readCount = 0;
        readCount = fis.read(buff);
        while (readCount != -1) {
            outputStream.write(buff, 0, readCount);
            readCount = fis.read(buff);
        }
        if (fis != null) {
            try {
                fis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
//        if (fs != null) {  //避免fs关闭的时候 ，线程还在执行 。所以这里不手动关闭。
//            try {
//                fs.closeAll();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//
//        }

    }

    static class MyThread  extends  Thread {
        List<File> file ;
        MyThread(List<File> file){
            this.file=file;
        }
        public void run() {
            for(int i=0;i<file.size();i++){
                try {

                    if(file.get(i).isDirectory()){
                        String   dst ="hdfs://139.199.172.112:9000/home/"+file.get(i).getAbsolutePath().replace("\\","/").replace("E:/","");
                        if(!fs.exists(new Path(dst))){
                            fs.mkdirs(new Path(dst));
                        }
                    }else{
                        String   dst ="hdfs://139.199.172.112:9000/home/"+file.get(i).getAbsolutePath().replace("\\","/").replace("E:/","");
                        System.out.println(dst+"target "+file.get(i).getAbsolutePath()+"source=");
                        CreateFile(dst,file.get(i).getAbsolutePath());

                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
                //     fs.copyFromLocalFile(new Path(file.get(i).getAbsolutePath()),uploadPath );
//                    if(file.get(i).isDirectory()){
//                        Path dir = new Path("hdfs://139.199.172.112:9000/home"+"/"+file.get(i).getAbsolutePath());
//                        if (!fs.exists(dir)){
//                            fs.mkdirs(dir);
//                        }
//                    }else{
//                       int j=file.get(i).getAbsolutePath().lastIndexOf("\\");
//                        Path filedir=new Path ("hdfs://139.199.172.112:9000/home"+"/"+file.get(i).getAbsolutePath().substring(0,j));
//                        Path upfile = new Path("hdfs://139.199.172.112:9000/home"+"/"+file.get(i).getAbsolutePath());
//                        if (!fs.exists(filedir)){
//                            fs.mkdirs(filedir);
//
//                        }
//                       fs.copyFromLocalFile(new Path(file.get(i).getAbsolutePath()),upfile );
//                    }



            }
            //上传文件
        }
    }

}
