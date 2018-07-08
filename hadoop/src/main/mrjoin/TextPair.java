import java.io.DataInput;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
public class TextPair implements WritableComparable<TextPair> {
    private    Text first;//Text 类型的实例变量 first
    private    Text second;//Text 类型的实例变量 second
    
    public TextPair() {
        set(new Text(),new Text());
    }
    
    public TextPair(String first, String second) {
        set(new Text(first),new Text(second));
    }
    
    public TextPair(Text first, Text second) {
        set(first, second);
    }
    
    public void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }
    
    public Text getFirst() {
        return first;
    }
    
    public Text getSecond() {
        return second;
    }
    
    //将对象转换为字节流并写入到输出流out中
    public void write(DataOutput out)throws IOException {
        first.write(out);
        second.write(out);
    }
    
    //从输入流in中读取字节流反序列化为对象
    public void readFields(DataInput in)throws IOException {
        first.readFields(in);
        second.readFields(in);
    }
    
    @Override
    public int hashCode() {
        return first.hashCode() *163+second.hashCode();
    }
    
    
    @Override
    public boolean equals(Object o) {
        if(o instanceof TextPair) {
            TextPair tp = (TextPair) o;
            return first.equals(tp.first) && second.equals(tp.second);
        }
            return false;
    }
    
    @Override
    public String toString() {
        return first +"\t"+ second;
    }

	public int compareTo(TextPair o) {
// TODO Auto-generated method stub
		if(!first.equals(o.first)){
			return first.compareTo(o.first);
		}
		else if(!second.equals(o.second)){
			return second.compareTo(o.second);
		}else{
			return 0;
 	}
	}
    
    //排序
    
	 

	 
}