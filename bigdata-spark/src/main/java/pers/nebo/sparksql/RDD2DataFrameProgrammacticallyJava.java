package pers.nebo.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2019/8/19
 * @ des : 使用编程的方式将RDD 转为DataFrame Java版
 */
public class RDD2DataFrameProgrammacticallyJava {
    public static void main(String[] args) {
       /* 1.6.x api
        SparkConf conf = new SparkConf();
        conf.setAppName("RDD2DataFrameProgrammactically");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> personRDD = sc.textFile("hdfs://hadoop1:9000/examples/src/main/resources/people.txt");
        /**
         * 是从数据库里面动态获取从来的
         * 在实际的开发中我们需要写另外的代码去获取
         */
//        String schemaString="name	age";
//        //create  schema
//        ArrayList<StructField> list = new ArrayList<StructField>();
//        for(String str:schemaString.split("\t")){
//            list.add(DataTypes.createStructField(str, DataTypes.StringType, true));
//        }
//        StructType schema = DataTypes.createStructType(list);
//        /**
//         * 需要将RDD转换为一个JavaRDD《Row》
//         */
//        JavaRDD<Row> rowRDD = personRDD.map(new Function<String, Row>() {
//
//            public Row call(String line) throws Exception {
//                String[] fields = line.split(",");
//
//                return RowFactory.create(fields[0],fields[1]);
//            }
//        });
//        DataFrame personDF = sqlContext.createDataFrame(rowRDD, schema);
//        personDF.registerTempTable("person");
//
//
//        DataFrame resultperson = sqlContext.sql("select name,age from person where age > 13 and age <= 19");
//        resultperson.javaRDD().foreach(new VoidFunction<Row>() {
//
//            private static final long serialVersionUID = 1L;
//
//            public void call(Row row) throws Exception {
//                //把每一条数据都看成是一个row  row(0)=name  row(1)=age
//                System.out.println("name"+row.getString(0));
//                System.out.println("age"+row.getInt(1));
//            }
//        });
//
//        resultperson.javaRDD().saveAsTextFile("hdfs://hadoop1:9000/reflectionresult");

        SparkConf conf = new SparkConf();
        conf.setAppName("RDD2DataFrameProgrammactically");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession ss= SparkSession.builder().appName("").config(conf).getOrCreate();
        JavaRDD<String> personRDD = sc.textFile("hdfs://hadoop1:9000/examples/src/main/resources/people.txt");

        String schemaString="name   age";

        //create  schema
        List<StructField> list= new ArrayList<>();
        for(String str:schemaString.split("\t")){
            list.add(DataTypes.createStructField(str,DataTypes.StringType,true));
        }

        StructType schema = DataTypes.createStructType(list);
        /**
         * 需要将RDD转换为一个JavaRDD《Row》
         */

        JavaRDD<Row> rowRDD=personRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {
                String[] fields=line.split(",");

                return RowFactory.create(fields[0],fields[1]);
            }
        });

        Dataset<Row> personDF = ss.createDataFrame(rowRDD, schema);
        personDF.registerTempTable("person");


        Dataset<Row> resultperson = ss.sql("select name,age from person where age > 13 and age <= 19");
        resultperson.javaRDD().foreach(new VoidFunction<Row>() {

            private static final long serialVersionUID = 1L;

            public void call(Row row) throws Exception {
                //把每一条数据都看成是一个row  row(0)=name  row(1)=age
                System.out.println("name"+row.getString(0));
                System.out.println("age"+row.getInt(1));
            }
        });

        resultperson.javaRDD().saveAsTextFile("hdfs://hadoop1:9000/reflectionresult");





    }

}
