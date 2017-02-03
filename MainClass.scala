//package raghav.hadoop.learning.spark.nse;

import java.util.Calendar

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
//import spark.implicits
//import org.apache.spark.implicits._

object RLogger extends Serializable {      
   @transient lazy val log = Logger.getLogger(getClass.getName)
}

object MainClass {

  def  main(args: Array[String]): Unit = 
  {
    val stTime = Calendar.getInstance().getTime()
    RLogger.log.info(stTime+"::Starting the processing of NSE Data")
    

  
      val spark = SparkSession
          .builder()
          //.master("spark://RaghavMacBookPro:7077")
          .master("local[2]")
          .appName("Spark App 2")
          .getOrCreate()
        
        
      // val spark = SparkSession.builder.config(new SparkConf()).getOrCreate() 
       //val sconf = new SparkConf().setMaster("local[*]").setAppName("NSE Analysis 1") 
       //val sc = new SparkContext(sconf)
      
      //val tfs = spark.textFile(
      val csvFilePath = "hdfs://localhost:9000/nsedata"
      
      val dataFrame = spark.read.format("CSV").option("header","true").load(csvFilePath)
      
      
       
       val m1 = dataFrame.select("SYMBOL","CLOSE","TIMESTAMP")
       
       val df2 = m1.selectExpr("SYMBOL", "cast(CLOSE as float) CLOSE", "TIMESTAMP")
        val maxdf = df2.groupBy("SYMBOL","TIMESTAMP").max("CLOSE")
        val mxNames = Seq("SYMBOL", "MxTMSTMP", "MxPrice")
        val maxdf2 = maxdf.toDF(mxNames: _*)
      
      
      val mindf = df2.groupBy("SYMBOL","TIMESTAMP").min("CLOSE")
      val mnNames = Seq("SYMBOL", "MnTMSTMP", "MnPrice")
      val mindf2 = mindf.toDF(mnNames: _*)
      
      mindf2.registerTempTable("MinT2")
      maxdf2.registerTempTable("MaxT2")
      
     val dfm = spark.sql("""
     select mn.SYMBOL, MnTMSTMP, MnPrice, MxTMSTMP, MxPrice
     from MinT2 as mn, MaxT2 as mx
     where mn.SYMBOL = mx.SYMBOL
     """.stripMargin)
     
      
     
     dfm.write.option("header","true").csv("/nsedata/filteredDataSet")
      //dfm.show()
    
      
      
      val cnts = dataFrame.count()
      RLogger.log.info("nse row count in total "+cnts)
      
      
      RLogger.log.info(Calendar.getInstance().getTime()+"::Finished the processing of NSE Data")
      RLogger.log.info(Calendar.getInstance().getTime()+"::Processing had started at " + stTime)
        
                
   
  }
   
}