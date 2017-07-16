
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CountLines {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("LinesCount").setMaster("local")
    val sc = new SparkContext(conf)
    // Read the source file
    val input = sc.textFile(args(0))
    // Gets the number of entries in the RDD
    val line = input.count()
    println("Total lines in " + args(0) + " is " + line)
    sc.stop
  }
}
