package utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class SettingUtil {



    public static String getKey(String filename, String key){
        Properties prop = new Properties();
        String value = null;
        try {

               InputStream path= SettingUtil.class.getResourceAsStream("/"+filename);
               System.out.println(path);
                prop.load( path);
               value = prop.getProperty(key);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return  value;

    }

    public static void main(String[] args) {
       System.out.println(getKey("hostsetting","txynebo1"));
    }
}
