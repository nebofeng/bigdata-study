package pers.nebo.hdfs.filetest;

import java.io.File;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 作用： 上传指定文件夹下的所有文件到hdfs
 */
public class FileUpload {


    File or;
    File[] files;
    static ExecutorService exe = Executors.newCachedThreadPool();




    public static void main(String []args) throws InterruptedException{

        String localFilePath= "D:\\SoftWareInstall";
//        long startTime=System.currentTimeMillis();   //获取开始时间
//          iteratorPath(localFilePath);
//          exe.shutdown();
//         while (true) {
//            if (exe.isTerminated()) {
//                System.out.println("结束了！");
//                break;
//            }
//            Thread.sleep(200);
//        }
//        long endTime=System.currentTimeMillis(); //获取结束时间
//
//        System.out.println("多线程程序运行时间： "+(endTime-startTime)+"ms");

        iteratorPath3(localFilePath);

//        long startTime2=System.currentTimeMillis();   //获取开始时间
//        iteratorPath2(localFilePath);
//
//        long endTime2=System.currentTimeMillis(); //获取结束时间
//
//        System.out.println("单线程程序运行时间： "+(endTime2-startTime2)+"ms");








        // 递归遍历文件夹



          //如果是文件夹 ， hdfs上新建文件夹  ，
        //   开启多线程池     开始继续遍历
        //   如果是文件     文件目录/文件名。
        //放入到 队列中 。

        //这是生产者



        //消费者 ，
        //从消费队列中获取 。
        //然后开始

    }

    /**
     * 上传文件夹的文件到hdfs（1.目录结构相同，2.只是上传文件 ）
     * @param path
     */
    /*
    思路： 获取path，如果是文件夹，则在hdfs指定文件夹下新建该文件夹
                     是文件 ，上传到当前文件夹 （文件目录结构为： hdfs指定文件夹/本地指定文件夹/ ）

                多线程上传文件 ：
                获取之后上传 ： 文件大小。大文件多线程上传 。
       思路： 边遍历。边上传。还是遍历之后上传。
        看文件大小 。大的多线程。


        方法 1 ： 多线程遍历 。存储到 容器中 。然后 再上传 。






     */
    public static  void traverseFolder1(String path) {
        int fileNum = 0, folderNum = 0;
        File file = new File(path);
        if (file.exists()) {
            LinkedList<File> list = new LinkedList<File>();
            File[] files = file.listFiles();
            for (File file2 : files) {
                if (file2.isDirectory()) {
                    System.out.println("文件夹:" + file2.getAbsolutePath());
                    list.add(file2);
                    fileNum++;
                } else {
                    System.out.println("文件:" + file2.getAbsolutePath());
                    folderNum++;
                }
            }
            File temp_file;
            while (!list.isEmpty()) {
                temp_file = list.removeFirst();
                files = temp_file.listFiles();
                for (File file2 : files) {
                    if (file2.isDirectory()) {
                        System.out.println("文件夹:" + file2.getAbsolutePath());
                        list.add(file2);
                        fileNum++;
                    } else {
                        System.out.println("文件:" + file2.getAbsolutePath());
                        folderNum++;
                    }
                }
            }
        } else {
            System.out.println("文件不存在!");
        }
        System.out.println("文件夹共有:" + folderNum + ",文件共有:" + fileNum);

    }




    public static  void iteratorPath(String dir) {
        System.out.println(dir);
       File or=new File(dir);
       File[] files = or.listFiles();
       if(files!=null){//dir是一个不为空的文件夹 。
           for(File file:files){
              if(file.isFile()){
                  System.out.println(file.getAbsolutePath());
                  //上传或者放到别的容器中
              } else{//是一个文件夹 。
                  MyThread newThread = new MyThread(file);
                // Thread thread = new Thread(newThread);
                  exe.execute(newThread);
                //newThread.start();
              }

           }

       }else{//dir是空文件夹或者是一个文件 。

           if(or.isDirectory()){//如果是个文件夹则说明 文件夹为空
              //新建文件夹

           }else{//说明这是一个文件
             //将文件 上传到hdfs中 。
               System.out.println(or.getAbsolutePath());
           }

       }
    }



    public static  void iteratorPath2(String dir) {
        System.out.println(dir);
        File or = new File(dir);
        File[] files = or.listFiles();
        if (files != null) {//dir是一个不为空的文件夹 。
            for (File file : files) {
                if (file.isFile()) {
                    System.out.println(file.getAbsolutePath());
                    //上传或者放到别的容器中
                } else {//是一个文件夹 。
                    iteratorPath2(file.getAbsolutePath());

                }

            }
        }else{//dir是空文件夹或者是一个文件 。

                if (or.isDirectory()) {//如果是个文件夹则说明 文件夹为空
                    //新建文件夹

                } else {//说明这是一个文件
                    //将文件 上传到hdfs中 。
                    System.out.println(or.getAbsolutePath());
                }

            }
        }




    public static  void iteratorPath3(String dir) {
        System.out.println(dir);
        File or=new File(dir);
        File[] files = or.listFiles();
        if(files!=null){//dir是一个不为空的文件夹 。
            for(File file:files){
                if(file.isFile()){
                    System.out.println(file.getAbsolutePath());
                    //上传或者放到别的容器中
                } else{//是一个文件夹 。
                    MyThread newThread = new MyThread(file);
                     newThread.start();
                }

            }

        }else{//dir是空文件夹或者是一个文件 。

            if(or.isDirectory()){//如果是个文件夹则说明 文件夹为空
                //新建文件夹

            }else{//说明这是一个文件
                //将文件 上传到hdfs中 。
                System.out.println(or.getAbsolutePath());
            }

        }
    }
    static class MyThread extends   Thread {
        File file ;
        MyThread(File file){
            this.file=file;
        }
        public void run() {
            iteratorPath(file.getAbsolutePath());//递归
        }
    }

    }
