package org.inceptez.hackathon

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types._;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import java.sql.Date;
import org.apache.spark.SparkConf;
import org.apache.spark.sql._;

import org.inceptez.hack.AllMethods

case class insuranceinfo(IssuerId: Int,IssuerId2: Int,BusinessDate: Date,StateCode: String,SourceName: String,NetworkName: String,
    NetworkURL: String,custnum: String,MarketCoverage: String,DentalOnlyPlan: String)
object obj1 {
  
  def main(args:Array[String]) {
    
    // PART 12
      val spark = SparkSession.builder().appName("Spark Hackathon 2019").master("local[*]")
    .config("hive.metastore.uris","thrift://localhost:9083").config("spark.hadoop.validateOutputSpecs", "false")
    .config("spark.sql.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate();
    spark.sparkContext.setLogLevel("error")
    
    val sc=spark.sparkContext;
      val insuredata= sc.textFile("hdfs://localhost:54310/user/hduser/sparkhack2/insuranceinfo1.csv")
      println("*****************Cleanup Started For Insurance 1 Information File********************")
      val firstLine = insuredata.first()
     firstLine.foreach(print)
      val filterlines = insuredata.filter(l => (l.trim().length != 0 && l != firstLine))
      .map(x => x.replaceAllLiterally("\"","|")).map(x => x.replaceAllLiterally(", ","#")).map(x => x.replaceAllLiterally(",,",""))
      println("Row Count with blank lines and Header:" + insuredata.count()) 
      println("Row Count without blank lines and Header:" + filterlines.count()) 
      filterlines.take(10).foreach(println)
      val rddsplit2=filterlines.map(x=>x.split(","))
      val filterlines2 = rddsplit2.filter(l => l.length == 10)
      println("Total Number of rows with 10 columns: " + filterlines2.count())
      val schemadInsuranceInfo1 = filterlines2.map(x => insuranceinfo(x(0).toInt,x(1).toInt, Date.valueOf(x(2)),x(3),x(4), x(5), x(6), x(7), x(8), x(9)))
      
      println("Total Number of rows before cleanup process: " + insuredata.count())
      println("Total Number of rows after cleanup process: " + schemadInsuranceInfo1.count())
      println("Total Number of rows removed in cleanup process including header: " + (insuredata.count() - schemadInsuranceInfo1.count()))
      val invalidData = rddsplit2.filter(l => l.length != 10)
      println("Total Number of rows of invalid data: " + invalidData.count())
      println("*****************Cleanup Completed For Insurance 1 Information File********************")
      
      
      
      val insuredata2= sc.textFile("hdfs://localhost:54310/user/hduser/sparkhack2/insuranceinfo2.csv")
      println("*****************Cleanup Started For Insurance 2 Information File********************")
      val firstLine1 = insuredata2.first()
      firstLine1.foreach(print)
      val filterlines3 = insuredata2.filter(l => (l.trim().length != 0 && l != firstLine))
      .map(x => x.replaceAllLiterally("\"","|")).map(x => x.replaceAllLiterally(", ","#")).map(x => x.replaceAllLiterally(",,",""))
      println("Row Count with blank lines and Header:" + insuredata2.count()) 
      println("Row Count without blank lines and Header:" + filterlines3.count()) 
      filterlines3.take(10).foreach(println)
      val rddsplit3=filterlines3.map(x=>x.split(","))
      val filterlines4 = rddsplit3.filter(l => l.length == 10)
      println("Total Number of rows with 10 columns: " + filterlines4.count())
      val schemadInsuranceInfo2 = filterlines4.map(x => insuranceinfo(x(0).toInt,x(1).toInt, Date.valueOf(x(2)),x(3),x(4), x(5), x(6), x(7), x(8), x(9)))
      
      println("Total Number of rows before cleanup process: " + insuredata2.count())
      println("Total Number of rows after cleanup process: " + schemadInsuranceInfo2.count())
      println("Total Number of rows removed in cleanup process including header: " + (insuredata2.count() - schemadInsuranceInfo2.count()))
      val invalidData1 = rddsplit3.filter(l => l.length != 10)
      println("Total Number of rows of invalid data: " + invalidData1.count())
      println("*****************Cleanup Started For Insurance 2 Information File********************")
      
      //Part 2
      
      val insuredatamerged = schemadInsuranceInfo1.union(schemadInsuranceInfo2).cache();
      
      insuredatamerged.take(10).foreach(println)
      
      println("Total Number of rows in first RDD: " + schemadInsuranceInfo1.count())
      println("Total Number of rows in second RDD: " + schemadInsuranceInfo2.count())
      println("Total Number of rows in merged RDD: " + insuredatamerged.count())
      println ("Count Same:" + ((schemadInsuranceInfo1.count() + schemadInsuranceInfo2.count())==insuredatamerged.count()))
      
      val insuredatamergeddistinct = insuredatamerged.distinct();
      println("Total Number of rows with Duplicates: " + insuredatamerged.count())
      println("Total Number of rows without Duplicates: " + insuredatamergeddistinct.count())
      println("Total Number of duplicates removed: " + (insuredatamerged.count() - insuredatamergeddistinct.count()))
      val insuredatarepart = insuredatamergeddistinct.repartition(8);
      import spark.sqlContext.implicits._
      val rdd_20191001 = insuredatarepart.toDF().filter("BusinessDate =='2019-10-01'").rdd
      val rdd_20191002 = insuredatarepart.toDF().filter("BusinessDate =='2019-10-02'").rdd
      println("total rows for date 2019-10-01 is: " + rdd_20191001.count())
      println("total rows for date 2019-10-02 is: " + rdd_20191002.count())
      println("Uploading final data in HDFS at location user/hduser/sparkhack2/outputp12");
      
      invalidData.saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2/outputp12/invalidata1")
      invalidData1.saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2/outputp12/invalidata2")
      insuredatamerged.saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2/outputp12/insuredatamerged")
      rdd_20191001.saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2/outputp12/rdd_20191001")
      rdd_20191002.saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2/outputp12/rdd_20191002")
      
      println("Upload Complete**********************************");
      val structypeschema = StructType(List(StructField("IssuerId", IntegerType, true),
        StructField("IssuerId2", IntegerType, true),
        StructField("BusinessDate", DateType, true),
        StructField("StateCode", StringType, true),
        StructField("SourceName", StringType, true),
        StructField("NetworkName", StringType, true),
        StructField("NetworkURL", StringType, true),
        StructField("custnum", StringType, true),
        StructField("MarketCoverage", StringType, true),
        StructField("DentalOnlyPlan", StringType, true)))
      val insuredaterepartdf = spark.createDataFrame(insuredatarepart.toDF().rdd, structypeschema)
      println("********showing data after final step*******")
      insuredaterepartdf.show(10)
      
      //Part34
        
    val custviewschema = StructType(List(StructField("id", StringType, true),
        StructField("firstname", StringType, true),
        StructField("lastname", StringType, true),
        StructField("age", StringType, true),
        StructField("profession", StringType, true)))
        
    val stateviewschema = StructType(List(StructField("statecode", StringType, true),
        StructField("statedesc", StringType, true)))
    
    spark.sparkContext.setLogLevel("error")
    val sqlc = spark.sqlContext;
    
    val dfcsv = sqlc.read.format("csv")
    .option("header",true)
    .option("escape",",")
    .schema(structypeschema)
    .load("hdfs://localhost:54310/user/hduser/sparkhack2/insuranceinfo1.csv", "hdfs://localhost:54310/user/hduser/sparkhack2/insuranceinfo2.csv")

    dfcsv.show(10);
    dfcsv.printSchema();
    println(dfcsv.count())
    import org.apache.spark.sql.functions._
    val colNameMod=dfcsv.withColumnRenamed("StateCode", "stcd").withColumnRenamed("SourceName", "srcnm")
    .withColumn("issueridcomposite",concat(col("IssuerId"),lit(" "), col("IssuerId2")))
    .drop("DentalOnlyPlan").withColumn("sysdt", current_date()).withColumn("systs", date_format(current_timestamp(), "H:m:s")).na.drop()
    val charRemUdf = udf(new AllMethods().remspecialchar _)
    colNameMod.show(10)
    colNameMod.withColumn("ModNetworkName",charRemUdf(col("NetworkName"))).drop("NetworkName")
    
    colNameMod.write.mode("overwrite").json("hdfs://localhost:54310/user/hduser/sparkhack2/resultpart3json")
    colNameMod.write.mode("overwrite").option("header", true).option("delimiter", "~").csv("hdfs://localhost:54310/user/hduser/sparkhack2/resultpart3csv")
    
    
    colNameMod.createOrReplaceTempView("sparkhackathonthree")

    spark.sql("drop table if exists default.sparkhackathonthreehive")
    spark.sql("create table default.sparkhackathonthreehive as select * from sparkhackathonthree")
    
    println("Displaying hive data")
    
    val hivedf=spark.read.table("default.sparkhackathonthreehive")
    hivedf.show(5,false)
    
    val custStatesData= sc.textFile("hdfs://localhost:54310/user/hduser/sparkhack2/custs_states.csv")
     custStatesData.take(10).foreach(println)
    val custfilter = custStatesData.map(x=> x.split(",")).filter(l => l.length == 5)
    val statesfilter = custStatesData.map(x=> x.split(",")).filter(l => l.length == 2)
    val custMastInfoCSV = sqlc.read.format("csv")
    .load("hdfs://localhost:54310/user/hduser/sparkhack2/custs_states.csv")
    
    val custfilterdf = custMastInfoCSV.filter(x => !x.anyNull)
    val statesfilterdf = custMastInfoCSV.filter(x => x.anyNull).drop("_c2","_c3","_c4")
    
    custfilterdf.show()
    statesfilterdf.show()
    
    val custviewdata = spark.createDataFrame(custfilterdf.toDF().rdd, custviewschema)
    val stateviewdata = spark.createDataFrame(statesfilterdf.toDF().rdd, stateviewschema)
    custviewdata.createOrReplaceTempView("custview")
    stateviewdata.createOrReplaceTempView("statesview")
    colNameMod.createOrReplaceTempView("insureview")
    spark.udf.register("remspecialcharudf", new AllMethods().remspecialchar _)
    println("Clubbing Data from all the tables")
    val querydata = spark.sql("Select cv.id, cv.firstname, cv.lastname, cv.age, cv.profession, sv.statecode, "+
        "sv.statedesc, iv.IssuerId, iv.IssuerId2, iv.BusinessDate, iv.stcd, iv.srcnm, iv.NetworkName, iv.NetworkURL, "+
        "iv.custnum, iv.MarketCoverage, iv.issueridcomposite, iv.sysdt, iv.systs, "+
        "case when instr(NetworkURL, 'https') > -1 then 'https' when instr(NetworkURL, 'https') > -1 then 'http' else 'noprotocol' end as protocol,"
        +" remspecialcharudf(iv.NetworkName) as cleannetworkname, iv.sysdt as curdt," +
      " iv.systs as curts, year(iv.businessdate) as yr, month(iv.businessdate) as mth from insureview iv inner join statesview sv"+
      " on iv.stcd=sv.statecode inner join custview cv on iv.custnum=cv.id")
    querydata.show(10)
    
    println("Finding average age and count")
    querydata.createOrReplaceTempView("querydata");
    querydata.write.mode("overwrite").parquet("hdfs://localhost:54310/user/hduser/sparkhack2/outputp34/output.parquet")
    val averageData = spark.sql("select AVG(age) as AverageAge, count(*) as NumofProfessionals from querydata group by statedesc, protocol, profession")
    averageData.show(10)
    
    println("Writing to mysql")

    val prop=new java.util.Properties();
    prop.put("user", "root")
    prop.put("password", "root")
    
    averageData.write.option("driver", "com.mysql.jdbc.Driver").mode("overwrite").jdbc("jdbc:mysql://localhost/spark_hack2","insureinfo",prop)
     println("Writing to mysql Completed")
  }

}