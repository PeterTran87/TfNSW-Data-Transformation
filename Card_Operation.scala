import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.text._
import java.io._

object cardData {
	def main(args:Array[String]) {
		val spark = SparkSession.builder.appName("Card Data Transformation").getOrCreate()
		import spark.implicits._
		
		// Extract card information
		val files = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///media/sf_Opal/For Scala/0*_Cards.csv").drop("_c0").select($"x".alias("path"))
		val list = files.select("path").collect();
		var path = Array[String]();
		for (i <- list) path = path :+ i(0).toString;
		
		// Latest card summary
		val card_data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load(path(4))
		val card = card_data.withColumn("CARD_TYP_CD", regexp_replace($"CARD_TYP_CD", "Sgl Trip ([A-Za-z]+) ([A-Za-z]+).*", "Sgl Trip Ticket"))
		
		// Import data time object
		val targetFormat = new SimpleDateFormat("dd-MMM-yyyy")
		val originFormat = new SimpleDateFormat("yyyyMMdd")

		// Get total data count
		var dataCount = Seq[(String, Long)]()
		for (i <- 0 to path.length-1) {
			var date = path(i).substring(34,42)
			date = targetFormat.format(originFormat.parse(date))
			val count = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load(path(i)).count().toLong
			val record = (date, count)
			dataCount = dataCount :+ record
		}
	
		val json1 = "{" + """"Info"""" + ": " + dataCount.toDF("date", "cardCount").sort($"cardCount".asc).toJSON.collect().mkString("[", "," , "]") + "}"
		val pw1 = new PrintWriter(new File("dataCount.json"))
		pw1.write(json1)
		pw1.close()

		// Get card type count
		var cardCount = Seq[(String, String, Long)]()
		val abnormal = Array[String]("UNKNOWN", "HOTLISTED", "BLOCKED", "EXPIRED")
		for (i <- 0 to path.length-1) {
			var date = path(i).substring(34,42)
			date = targetFormat.format(originFormat.parse(date))
			val count = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load(path(i)).withColumn("DISC_ENT_DESC", when($"DISC_ENT_DESC" === "No DE", "No Discount").otherwise($"DISC_ENT_DESC")).withColumn("CARD_TYP_CD", regexp_replace($"CARD_TYP_CD", "Sgl Trip ([A-Za-z]+) ([A-Za-z]+).*", "Sgl Trip Ticket")).withColumn("CARD_TYP_CD", when($"DISC_ENT_DESC" === "No Discount" && $"CARD_TYP_CD" === "UNKNOWN", "Adult").otherwise($"CARD_TYP_CD")).withColumn("PSNGR_TYP_DESC", when($"CARD_TYP_CD" === "Child/Youth" && $"PSNGR_TYP_DESC" === "Adult" && $"DISC_ENT_DESC" === "No Discount", "Child/Youth").otherwise($"PSNGR_TYP_DESC")).withColumn("CARD_STAT_CD", when($"CARD_STAT_CD".isin(abnormal: _*), "TEMPORARY").otherwise($"CARD_STAT_CD")).groupBy("CARD_TYP_CD").count()
			val types = count.select("CARD_TYP_CD").collect()
			var userType = Array[String]()
			for (j <- types) userType = userType :+ j(0).toString
			val counts = count.select("count").collect()
			var typeCount = Array[Long]()
			for (j <- counts) typeCount = typeCount :+ j(0).toString.toLong
			for (j <- 0 to userType.length-1) {
				val record = (date, userType(j), typeCount(j))
				cardCount = cardCount :+ record
			}
		}

		val json2 = "{" + """"Info"""" + ": " + cardCount.toDF("date", "type", "typeCount").sort($"date".asc, $"type".asc).toJSON.collect().mkString("[", "," , "]") + "}"
		val pw2 = new PrintWriter(new File("cardCount.json"))
		pw2.write(json2)
		pw2.close()
	}
}
