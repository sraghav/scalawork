
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SparkSession
//import spark.implicits

case class Person(name: String, age: Long)


object SparkAppTest {
  def main(args : Array[String]) {

    val spark = SparkSession
      .builder()
      //.master("spark://RaghavMacBookPro:7077")
      .master("local[2]")
      .appName("Spark SQL Example")
      .getOrCreate()
      
  import spark.implicits._
  
  val mydf = spark.sparkContext
            .textFile("hdfs://localhost:9000/user/raghav/family")
            .map(_.split(","))
            .map(att => Person(att(0), att(1).trim.toInt))
            .toDF()
            
  mydf.createOrReplaceTempView("family")
  
  val myRes = spark.sql("Select name, age from family")
  
  myRes.write.save("mydfOut.parquet")
  //myRes.map(fm => "Name - "+fm(0)).collect()
  
  
  }
  
  
  def myfunc(args : Array[String]) {
    val conf = new SparkConf().setAppName("Spark test")
    conf.setMaster("spark://RaghavMacBookPro:7077")
    conf.setMaster("local[2]")
    //conf.setSparkHome("/hadoop/spark")
    conf.set("spark.driver.host","localhost")
    
    val spark = new SparkContext(conf)

    
    val f = spark.textFile("hdfs://localhost:54310/user/raghav/family")
    val ll = f.map(s => s.length).reduce((a,b) => a+b)
    
    /*val slices = if (args.length > 0) args(0).toInt else 20
    val n = math.min(100000L * slices, Int.MaxValue).toInt
    val count = spark.parallelize(1 until n, slices).map { i=>
      val x = scala.util.Random.nextFloat() * 2 -1
      val y = scala.util.Random.nextFloat() * 2 -1
      if (x*y + y*y < 1) 1 else 0
    }.reduce(_+_)
    * 
    */
    //println("Result =====> Pi is roughtly " + 4.0 * count / n + "=======")
    println("Result =====> Line length is "+ ll +"=======")
    spark.stop()
  }
}