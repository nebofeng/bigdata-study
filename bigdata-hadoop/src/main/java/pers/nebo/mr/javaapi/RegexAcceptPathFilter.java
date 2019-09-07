package pers.nebo.mr.javaapi;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

//过滤文件
public class RegexAcceptPathFilter implements PathFilter {
    private final String regex;

    public RegexAcceptPathFilter(String regex) {
        this.regex = regex;
    }


    public boolean accept(Path path) {

        boolean flag = path.toString().matches(regex);

        return flag;
    }
}