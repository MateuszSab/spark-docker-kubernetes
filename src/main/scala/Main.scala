import org.apache.spark.sql.DataFrame

object Main extends App {
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
//    .master("local[*]")
    .getOrCreate()

  if (args.length < 1) {
    println("ERROR: brak argumentu")
    sys.exit(-1)
  }
  val path = args(0)

  spark.read.text(path).show
}
