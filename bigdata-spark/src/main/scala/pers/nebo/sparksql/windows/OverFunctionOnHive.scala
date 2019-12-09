package pers.nebo.sparksql.windows

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @ author fnb
  * @ email nebofeng@gmail.com
  * @ date  2019/12/9
  * @ des :
  */
object OverFunctionOnHive {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("over").enableHiveSupport().getOrCreate()
    spark.sql("use db")
    spark.sql("create table if not exist sales(riqi string,leibie string,jine Int)"+"row format delimited  fields  terminated by '\t'")
    spark.sql("load data local inpath  '/datapath'")

    /**
      * rank在每个组内 从 1开始
      */
    val result =spark.sql(

      "select riqi,leibie,jine"
                        +"from ("
                          +"select "
                            +"riqi,leibie,jine" + "row number() over (partition by leibie order by jine desc) rank"
                              +"from sales) t"
                           +"where t.rank<=3"



    )

    result.write.mode(SaveMode.Append).saveAsTable("result")
    result.show(100)



  }

}
