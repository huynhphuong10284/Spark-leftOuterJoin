

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Calendar
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{ JdbcRDD, RDD }
import org.apache.spark.sql.cassandra.CassandraSQLContext

class Influencer_Extract extends BaseContext with Serializable {
  def main() {
    var conf = buildSparkConf("Influencer Extraction")
    var sc = new SparkContext(conf)
    var cc = new CassandraSQLContext(sc)
    cc.setKeyspace(KEYSPACE)

    val saveMYSQL = new LoadPostToMySQL()
    val new_url = "jdbc:mysql://172.31.43.234:3306/achoo_cp?user=" + username + "&password=" + password

    Class.forName(driver).newInstance
    var Influencer_data = new JdbcRDD(sc, () =>
      DriverManager.getConnection(url, username, password),
      "SELECT relative_path, handle FROM achoo_cp.channel LIMIT ?, ?",
      0, 100000000, 1, r => (r.getString("relative_path"), r.getString("handle")))
       .keyBy(f => f._1)
      .cache

    var import_data = sc.cassandraTable(KEYSPACE, INFLUENCER_NAME)
      .select("relativepath", "influencer_name", "platform")

      .map { x => (x.getString(0), x.getString(1), x.getString(2)) }
      .map {
        case (relative_path, influencer_name, platform) => (relative_path, influencer_name, platform)
      }
       .keyBy(f => f._1)
       
       var final_data = import_data.leftOuterJoin(Influencer_data)
      .map { case ((relative_path), ((_, influencer_name, platform), temp: Option[(_, handle)])) => (relative_path, influencer_name, platform, temp.fold("Null") { case (_, handle) => (handle) }) }
      .map { case (relative_path, influencer_name, platform, handle) => (relative_path, influencer_name, platform, handle) }
      .filter { case (_, _, _, handle) => (handle.equals("Null")) }
      .map { case (relative_path, influencer_name, platform, handle) => (relative_path, influencer_name, platform) }
      .distinct()
      .cache()
      println("--------------------------------------------------------------------------------------")
      final_data.collect().foreach(println)
      
      sc.stop()
       
       
  }
}