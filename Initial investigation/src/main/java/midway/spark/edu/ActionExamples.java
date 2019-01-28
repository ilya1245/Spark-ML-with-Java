package midway.spark.edu;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;


public class ActionExamples {

    private static String appName = "LOAD_DATA_APPNAME";
    private static String master = "local";
    private static String FILE_NAME = "univ_rankings.txt";

    public static void main(String[] args) {

        LogManager.getLogger("org").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rddX = sc.parallelize(Arrays.asList("big data","analytics", "using java"));
        List<String> strs = rddX.collect();
        for (String str : strs) {
            System.out.println(str);
        }


        /* Listing 3-6  Serialization ERROR!!!*/
/*    case class Contact(id: Long, name: String, email: String)
    val contactData = Array("1#John Doe#jdoe@domain.com", "2#Mary Jane#mjane@domain.com")
    val contactDataRDD = spark.sparkContext.parallelize(contactData)
    val contactRDD = contactDataRDD.map(l => {
      val contactArray = l.split("#")
      Contact(contactArray(0).toLong, contactArray(1), contactArray(2))
    })
    contactRDD.collect.foreach(println)*/
    }
}
