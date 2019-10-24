package pers.nebo.mr.hotday;

import com.sun.corba.se.spi.ior.Writeable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2019/10/23
 * @ des : 查询 数据中 ，每个月 温度最高的两天
 *  日期                温度
 * xxxx-xx-x            xx
 *
 */
public class HotDay  implements WritableComparable<HotDay> {
    private int year;
    private int month;
    private int day;
    private int wd;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getWd() {
        return wd;
    }

    public void setWd(int wd) {
        this.wd = wd;
    }

    /**

     * @param   o the object to be compared.
     * @return  a negative integer, zero, or a positive integer as this object
     *          is less than, equal to, or greater than the specified object.
     *
          负整数、零或正整数，标识 此对象小于、等于或大于指定对象。
     */
    @Override
    public int compareTo(HotDay o) {

        int flagy=Integer.compare(this.getYear(),o.getYear());
        if(flagy==0){
            int flagm= Integer.compare(this.getMonth(),o.getMonth());
            if(flagm==0){
                return Integer.compare(this.getDay(),o.getDay());
            }
            return flagm;
        }
        return flagy;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(month);
        out.writeInt(day);
        out.writeInt(wd);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year=in.readInt();
        this.month=in.readInt();
        this.day=in.readInt();
        this.wd=in.readInt();

    }
}
