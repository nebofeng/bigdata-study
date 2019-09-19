package pers.nebo.hdfs.rpcdemo;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2019/9/14
 * @ des : 协议
 */
public interface ClientProtocol {
    public static final long versionID=1L;
    /**
     * 创建目录
     * @param path
     */
    void mkdirPath(String path);

    /**
     * 获取文件
     * @param Path
     * @return
     */
    String getFile(String Path);
}
