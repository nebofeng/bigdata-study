package pers.nebo.hdfs.fseditlog;

import java.util.ArrayList;
import java.util.List;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2019/9/19
 * @ des :
 *   /root
 *          /a
 *              /a1
 *              /a2
 *         /b
 *              /b1
 *              /b2
 *
 *
 *
*/
public class FSDirectory {

    public static void main(String[] args) {
        INodeDirectory rootDir= new INodeDirectory("/root");
        INodeDirectory aDir= new INodeDirectory("/a");
        INodeDirectory bDir= new INodeDirectory("/b");
        INodeDirectory testa1= new INodeDirectory("/testa1");
        INodeDirectory testa2= new INodeDirectory("/testa2");
        INodeDirectory testb1= new INodeDirectory("/testb1");
        INodeDirectory testb2= new INodeDirectory("/testb2");

    }

    public static  class INodeDirectory{
        public String name;

        public List<INodeDirectory> children = new ArrayList<INodeDirectory>();

        public INodeDirectory(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<INodeDirectory> getChildren() {
            return children;
        }

        public void setChildren(ArrayList<INodeDirectory> children) {
            this.children = children;
        }
    }
}
