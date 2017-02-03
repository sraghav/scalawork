/**
  * Created by 322222480 on 2017/01/30.
  */



import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.log4j.LogManager
//import com.hdp.lrm.bo.{InputLoadExecutor, LRMExecutor}
//import com.hdp.lrm.utility.LRMUtility
import com.typesafe.config.ConfigFactory


object hiveTest1 {

  @transient lazy val log = LogManager.getLogger("myHTLogger")


  def main(args: Array[String]): Unit =
  {
    val startTime = System.currentTimeMillis()
    val conf = new SparkConf().setAppName("hiveTest1")
      .set("spark.serialization","org.apache.spark.serializer.KyroSerializer")
      .set("spark.io.compression.codec","org.apache.spark.io.SnappyCompressionCodec")
    //.set("master","yarn-cluster")
    //.set("spark.sql.shuffle.partitions","500")
    val sc = new SparkContext(conf)

    //val textFile = sc.textFile("/DEV/staging/LY00/logs/sqoop-ingestion-test.log")
    //val wordCounts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)

    //log.info("here are word counts in file " + wordCounts)


  }

}
