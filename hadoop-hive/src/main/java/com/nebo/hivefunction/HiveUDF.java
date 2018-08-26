package com.nebo.hivefunction;


import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class HiveUDF  extends UDF {

    public Text evaluate(final Text s) {
        if (s == null) { return null; }
        return new Text(s.toString().toLowerCase());
    }

}
