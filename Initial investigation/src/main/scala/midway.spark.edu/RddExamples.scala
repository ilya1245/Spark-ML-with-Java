package midway.spark.edu


import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession

object RddExamples {
  def main(args: Array[String]) {

    LogManager.getLogger("org").setLevel(Level.OFF)

    val stringList = Array("Spark is awesome", "Spark is cool")
    val spark = SparkSession.builder.master("local").appName("OutliersDetection").getOrCreate()

    println("")
    println("******* START ********")
    println("")

    val stringRDD = spark.sparkContext.parallelize(stringList)
    stringRDD.collect().foreach(println)

    /* Listing 3-3 */
    val allCapsRDD = stringRDD.map(line => line.toUpperCase)
    allCapsRDD.collect().foreach(println)

    /* Listing 3-5 */
    def toUpperCase(line: String): String = {
      line.toUpperCase
    }

    stringRDD.map(l => toUpperCase(l)).collect.foreach(println)

    /* Listing 3-6  Serialization ERROR!!!*/
/*    case class Contact(id: Long, name: String, email: String)
    val contactData = Array("1#John Doe#jdoe@domain.com", "2#Mary Jane#mjane@domain.com")
    val contactDataRDD = spark.sparkContext.parallelize(contactData)
    val contactRDD = contactDataRDD.map(l => {
      val contactArray = l.split("#")
      Contact(contactArray(0).toLong, contactArray(1), contactArray(2))
    })
    contactRDD.collect.foreach(println)*/

    /* Listing 3-8 */
    val stringLenRDD = stringRDD.map(l => l.length)
    stringLenRDD.collect.foreach(println)

    /* Listing 3-9 */
    val wordRDD = stringRDD.flatMap(line => line.split(" "))
    wordRDD.collect().foreach(println)

    /* Listing 3-11 */
    stringRDD.map(line => line.split(" ")(2)).collect.foreach(println)
    stringRDD.flatMap(line => line.split(" ")(1)).collect.foreach(println)

    /* Listing 3-14 */
    val awesomeLineRDD = stringRDD.filter(line => line.contains("awesome"))
    awesomeLineRDD.collect

    /* Listing 3-16 */
    println("******* Listing 3-16 ********")
    import scala.util.Random
    val sampleList = Array("One", "Two", "Three", "Four", "Five")
    val sampleRDD = spark.sparkContext.parallelize(sampleList, 2)
    var result = sampleRDD.mapPartitions((itr: Iterator[String]) => {
      val rand = new Random(System.currentTimeMillis +
        Random.nextInt)
      itr.map(l => l + ":" + rand.nextInt)
    })

    result.collect().foreach(println)

    /* Listing 3-18 */
    println("******* Listing 3-18 ********")
    import scala.util.Random
    def addRandomNumber(rows: Iterator[String]): Iterator[String] = {
      val rand = new Random(System.currentTimeMillis + Random.nextInt)
      rows.map(l => l + " : " + rand.nextInt)
    }
    /* Listing 3-19 */
    result = sampleRDD.mapPartitions((rows: Iterator[String]) => addRandomNumber(rows))
    result.collect().foreach(println)


  }
}
