
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 单个目录多个文件上传
 */
public class SingleMain {

    public static final String hdfsPath = "hdfs://10.0.0.200:8020";
    //static Path uploadPath = new Path(hdfsPath + "/home");
   // Path sourcePath = new Path("E:\\CodeDocument\\20180626test");
    static Configuration conf = new Configuration();
    static URI uri;
    static FileSystem fs;

    static {
        try {

            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            uri = new URI(hdfsPath);
            fs = FileSystem.get(uri, conf);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws IOException, URISyntaxException {
        //  CreateFile(uploadPath+"/13.txt","E:\\CodeDocument\\20180626test\\1.txt");
        String uploadHdfs = "";
        String uploadPath = "";

        long startTime=System.currentTimeMillis();   //获取开始时间
        String filePath = "/home/nebo/sdk_ros_ir";
        int coreSize;
        if(args[0]==null){
            System.out.println("输入错误。请输出线程数目 ，p：当总文件个数小于 线程个数的时候。 线程个数为文件个数 。");
        }
          coreSize=  Integer.parseInt(args[0]);//Runtime.getRuntime().availableProcessors()*2+1;//io密集的 程序为2n+1 ,n为 cpu个数 ，cpu密集则为 n+1
            // n*（x+y）/x  本机计算时间为x ，等待时间为y  ;


        List<File> fileArrayList = new ArrayList<File>();

        ExecutorService exe = Executors.newFixedThreadPool(coreSize);
        fileArrayList = SingleGetFilList.getFileList(filePath);
        if (fileArrayList.size() < coreSize) {
            coreSize = fileArrayList.size();
        }

        System.out.println("线程个数： ======="+coreSize+"        文件个数 ：====" +fileArrayList.size());
        int threadDealSize = fileArrayList.size() / coreSize;//不会等于零 。文件数目不会小于 。
        int moreSize = fileArrayList.size() % coreSize; //剩余不够平均分配的条件数据。
        int start = 0;
        for (int i = 0; i < moreSize; i++) {
//             MyThread newThread = new MyThread(fileArrayList.subList(start,start+threadDealSize+1));
//            newThread.start();
            exe.submit(new MyThread(fileArrayList.subList(start, start + threadDealSize + 1)));
            start = start + threadDealSize + 1;
        }
        for (int i = 0; i < coreSize - moreSize; i++) {
//            MyThread newThread = new MyThread(fileArrayList.subList(start,start+threadDealSize));
//            newThread.start();
            exe.submit(new MyThread(fileArrayList.subList(start, start + threadDealSize)));
            start = start + threadDealSize;
        }
        exe.shutdown();
        long endTime=System.currentTimeMillis(); //获取结束时间
        System.out.println("多线程程序运行时间： "+(endTime-startTime)+"ms");
    }


    /**
     * 根据传入的两个目录 ，写文件到指定目录
     * @param dst
     * @param src
     * @throws IOException
     */
    public static void CreateFile(String dst, String src) throws IOException {

//        FileInputStream fis = null;
//        fis = new FileInputStream(src);
        BufferedReader bf = new BufferedReader(new FileReader(new File(src)));
        String line = null;
        StringBuilder sb = new StringBuilder();
        while ((line = bf.readLine()) != null) {
            sb.append(line);
        }
        bf.close();
        //获取了字符串内容 sb
        String[] parms = (sb.toString()).split("\t");
        if (parms.length == 5 && parms[3].length() == 8) {
            String nameSpace = parms[2] + "/" + parms[3].substring(0, 3) + "/" + parms[3].substring(4, 5) + "/" + parms[3].substring(6, 7) + "/";

        } else {   //不符合格式的log处理逻辑

        }
        // dst = dst+ "/" //原本指定的文件夹+根据文件内容拼凑的子级目录+ 文件名
        Path dstPath = new Path(dst);//目标路径
        FSDataOutputStream outputStream = fs.create(dstPath);


        outputStream.write("开始处理===》start+\r\n".getBytes());
        //已知log文本仅有一行。这里暂时不做修改。
        while (line != null) {
            ///对line进行处理 ，
            //  fsDataOutputStream.write(resultStr.getBytes());
            outputStream.write((new String(line).replace("未处理", "weichuli") + "\r\n").getBytes());
            sb.append(line);
            line = bf.readLine();

        }
        outputStream.write("结束处理===》end".getBytes());
        //此时的流程。先读取文件内容 。然后根据内容设置文件目录 。在读取内容 。 此时sb是内容的字符串形式 。


//       字节流
        //
//        byte[] buff = new byte[1024];
//        int readCount = 0;
//        readCount = fis.read(buff);
//        while (readCount != -1) {
//            outputStream.write(buff, 0, readCount);
//            readCount = fis.read(buff);
//        }

        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 根据传入的路径， 以及 file对象。将file对象传入处理后的目录
     *
     * @param dst
     * @param src
     * @throws IOException
     */
    public static void CreateFile(String dst, File src) throws IOException {

        if (!src.isDirectory()) {
            BufferedReader bf = new BufferedReader(new FileReader(src));
            String line = null;
            StringBuilder sb = new StringBuilder();
            while ((line = bf.readLine()) != null) {
                sb.append(line);
            }
            bf.close();
            //获取了字符串内容 sb
            String[] parms = (sb.toString()).split("\\s+");
            String nameSpace = null;
            if (parms.length == 5 && parms[3].length() == 8) {
                nameSpace = parms[2] + "/" + parms[3].substring(0, 4) + "/" + parms[3].substring(4, 6) + "/" + parms[3].substring(6, 8) + "/";

            } else {   //不符合格式的log处理逻辑

            }
            int j = src.getAbsolutePath().lastIndexOf("/");
            dst = dst + "/" + src.getAbsolutePath().substring(0, j) + "/" + nameSpace + "/" + src.getName(); //原本指定的文件夹+原来的文件夹 +根据文件内容拼凑的子级目录+ 文件名
            Path dstPath = new Path(dst);//目标路径
            FSDataOutputStream outputStream = fs.create(dstPath);
            outputStream.write("开始处理===》start+\r\n".getBytes());
            //已知log文本仅有一行。这里暂时不做修改。

            line =null;
            BufferedReader oldBf = new BufferedReader(new FileReader(src));
            while ((line = oldBf.readLine()) != null) {
                 ///对line进行处理 ，
                //  fsDataOutputStream.write(resultStr.getBytes());
                outputStream.write((new String(line).replace("未处理", "weichuli") + "\r\n").getBytes());

            }
            oldBf.close();
            outputStream.write("结束处理===》end".getBytes());
            //此时的流程。先读取文件内容 。然后根据内容设置文件目录 。在读取内容 。 此时sb是内容的字符串形式 。


//       字节流
            //
//        byte[] buff = new byte[1024];
//        int readCount = 0;
//        readCount = fis.read(buff);
//        while (readCount != -1) {
//            outputStream.write(buff, 0, readCount);
//            readCount = fis.read(buff);
//        }

            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else {//是个文件夹 。直接创建文件夹
            if (!fs.exists(new Path(dst))) {
                fs.mkdirs(new Path(dst));
            }
        }
    }

    /**
     * 如果是目录就根据目录新建 ，如果是文件。就根据文件解析目录 。
     * @param dst
     * @param src
     * @throws IOException
     */
    public static void CreateFile2(String dst, File src) throws IOException {

        if (!src.isDirectory()&&src.getName().length()==18) {//校验是 2000 00 00 00 00 00 .txt (年月日 时分秒 .txt 的18个字符的名字 )
            String fileName = src.getName();//get file name
            String nameSpace = "/"+fileName.substring(0,4)+"/"+fileName.substring(4,6)+"/"+fileName.substring(6,8)
                             +"/"+fileName.substring(8,10)+"/" ;//文件分到小时 。

            //linux 文件 目录，  /home/nebo/sdk_ros_ir/
         //   src.getCanonicalPath()
            String prNameSpace = src.getAbsolutePath().split("/")[3];
            dst = dst+"/home/nebo/logdata/"+prNameSpace+nameSpace+src.getName();//路径+文件名



            Path dstPath = new Path(dst);//目标路径
            FSDataOutputStream outputStream = fs.create(dstPath);
            //outputStream.write("开始处理===》start+\r\n".getBytes());
            //已知log文本仅有一行。这里暂时不做修改。

            String line = null;
            BufferedReader oldBf = new BufferedReader(new FileReader(src));
            while ((line = oldBf.readLine()) != null) {
                ///对line进行处理 ，
                outputStream.write((new String(line).replace("未处理", "weichuli") + "\r\n").getBytes());

            }
            oldBf.close();
         //   outputStream.write("结束处理===》end".getBytes());
            //此时的流程。先读取文件内容 。然后根据内容设置文件目录 。在读取内容 。 此时sb是内容的字符串形式 。

            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else {//是个文件夹 。直接创建文件夹
            if (!fs.exists(new Path(dst))) {
                fs.mkdirs(new Path(dst));
            }
        }
    }



    static class MyThread extends Thread {
        List<File> file;

        MyThread(List<File> file) {
            this.file = file;
        }

        public void run() {
            for (int i = 0; i < file.size(); i++) {
                try {
//                    if(file.get(i).isDirectory()){
//                     //   String   dst =uploadPath+"/"+file.get(i).getAbsolutePath().replace("\\","/").replace("E:/","");
//                        if(!fs.exists(new Path(dst))){
//                            fs.mkdirs(new Path(dst));
//                        }
//                    }else{
                    //   String   dst =uploadPath+"/"+file.get(i).getAbsolutePath().replace("\\","/").replace("E:/","");
                    CreateFile2(hdfsPath, file.get(i));
//                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
