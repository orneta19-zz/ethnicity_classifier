/**
  * Created by Neta on 09/04/2016.
  */

  import org.apache.spark.rdd.RDD
  import org.apache.spark.{SparkConf, SparkContext}
  import org.joda.time.format.DateTimeFormat
  import org.joda.time.{DateTimeZone, DateTime}
  import org.slf4j.{LoggerFactory, Logger}
  import scala.collection.JavaConversions._

  object text2ethnicity {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)

    def main(args: Array[String]): Unit = {
      val sc = createSparkContext(Configuration.LOCAL)
      val runTime: String = DateTime.now(DateTimeZone.UTC).toString(DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm"))
      val inputPath = if (Configuration.LOCAL) "..." else "..."
      val outputPath = if (Configuration.LOCAL) "..." + runTime else "..." + runTime

      val userdataText = sc.textFile(inputPath)

      /** "twitterData" gets input from 'user-data' and filters it to get only data from Twitter.
        * It filters the data again to get only users which had a "description".
        * It gets their UserID (Str) and the description they wrote about themselves.
        */
      case class descriptionFound(userId: String, description: String)
      val twitterData = userdataText
        .map(JsonParser.fromJson)
        .filter(node =>node.has("data") && node.get("data").has("twitter") && node.get("data").get("twitter").has("source") &&
          node.get("data").get("twitter").get("source").asText().equals("twitter"))
        .filter(node => node.get("data").get("twitter").get("rawData").has("description"))
        .map(node => {
          val description = node.get("data").get("twitter").get("rawData").get("description").asText()
          if (description.nonEmpty)
            descriptionFound(node.get("data").get("twitter").get("id").asText(), description)
          else
            null
        })
        .filter(_ != null)

      /** 'tagged' takes its input from 'twitterData' and the regex lists from 'Ethnicity'.
        * It checks what is the users' ethnicity-according to their bio.
        * result: userId, description, tagged.
        */
      case class tagged(userId: String, description: String, tagged: String)
      val ethnicityFound = twitterData
        .map(descfi =>
          tagged(descfi.userId, descfi.description, Ethnicity.description2ethnicity(descfi.description)))
        .filter(_.tagged != null)

      ethnicityFound.saveAsTextFile(outputPath + "/ethnicityFound")


      sc.stop()

    }


    def createSparkContext(isLocal: Boolean, description: String = "text2ethnicity", pool: String = "slow"): SparkContext = {
      val sparkConf = new SparkConf().setAppName("Research - " + description)

      if (isLocal) {
        sparkConf.setMaster("local[4]")
        sparkConf.set("spark.driver.allowMultipleContexts", "true")
      }

      val sparkContext = new SparkContext(sparkConf)
      sparkContext.setLocalProperty("spark.scheduler.pool", pool)

      return sparkContext
    }

}
