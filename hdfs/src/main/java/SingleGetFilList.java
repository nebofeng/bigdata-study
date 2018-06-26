import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class SingleGetFilList {
    private static ArrayList<File> fileList  = new ArrayList<File>();
  public static  ArrayList<File>  getFileList(String filePath){


    File file = new File(filePath);
        if(!file.isDirectory()){
        System.out.println("文件【" + file.getAbsolutePath() + "】：" + file.getAbsolutePath());
        fileList.add(file);
    }else{
        System.out.println("文件夹【" + file.getName() + "】：" + file.getAbsolutePath());
        File[] files = file.listFiles();
        for(int i = 0; i < files.length; i++){
            if (!files[i].isDirectory()) {
                System.out.println("　　文件【" + files[i].getName() + "】："+files[i].getAbsolutePath());
                fileList.add(files[i]);
            } else if (files[i].isDirectory()) {
                getFileList(files[i].getAbsolutePath());
            }
        }
        if(files.length==0){//空文件夹
            fileList.add(file);

        }

    }
        return fileList;
 }


}


//每个文件夹下面的文件。都新建一个队列。