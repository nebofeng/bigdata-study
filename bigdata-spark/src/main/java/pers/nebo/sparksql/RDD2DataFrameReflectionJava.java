package pers.nebo.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


/**
 * @ author fnb
 * @ email nebofeng@gmail.com
 * @ date  2019/8/19
 * @ des : RDD2DataFrameReflectionJava
 */
public class RDD2DataFrameReflectionJava {
    public static void main(String[] args) {

    }

    public void RDD2DataFrameReflectionJava(){
        SparkConf sc=new SparkConf().setAppName("RDD2DataFrameReflectionJava")
                .setMaster("local");
        JavaSparkContext jsc=new JavaSparkContext(sc);

        /* 1.6.x api
        SparkConf conf = new SparkConf();
		conf.setAppName("RDD2DataFrameReflection");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
         */
        SparkSession ss=SparkSession.builder()
                            .appName("tmp")
                             .config(sc)
                            .getOrCreate();



        JavaRDD<Person> PersonRDD = jsc.textFile("hdfs://hadoop1:9000/examples/src/main/resources/people.txt")
                .map(new Function<String, Person>() {

                    public Person call(String line) throws Exception {
                        String[] strs = line.split(",");
                        String name=strs[0];
                        int age=Integer.parseInt(strs[1].trim());
                        Person person=new Person(age,name);
                        return person;
                    }

                });
        Dataset<Row> personDataset=ss.createDataFrame(PersonRDD, Person.class);
//        DataFrame
//        DataFrame personDF = ss.createDataFrame(PersonRDD, Person.class);
        personDataset.registerTempTable("person");

//        DataFrame resultperson = sqlContext.sql("select name,age from person where age > 13 and age <= 19");
        Dataset<Row> resultperson  =ss.sql("select name,age from person where age > 13 and age <= 19");
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
