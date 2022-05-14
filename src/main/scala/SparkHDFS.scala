import org.apache.spark.sql.SparkSession

object SparkHDFS {
  def main(args: Array[String]) = {

    if (args.length < 1) {
      println("ERROR: brak argumentu")
      sys.exit(-1)
    }
    val path = args(0)

    println(s"loading files from: $path")

    implicit val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .getOrCreate()

    filesFromPath(path).show(false)
  }

  def filesFromPath(path: String)(implicit spark: SparkSession) = {
    spark.read.text(path)
  }
}
