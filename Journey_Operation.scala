package org.apache.spark.sql.expressions

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions._
import java.text._
import java.io._

object journeyData {
	def main(args:Array[String]) {
		val spark = SparkSession.builder.appName("Journey Data Transformation").getOrCreate()
		import spark.implicits._

		// Extract journey information
		val data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load("file:///media/sf_Opal/Journey/*.csv")

		// Take necessary columns from original datasets
		val dataSelected = data.filter($"TAG1_DT_FK" =!= "20160718").select("CARD_FK", "PSNGR_TYP_CD", "CARD_TYP_CD", "JRNY_ID", "DISC_ENT_DESC", "TS_TYP_CD", "JS_FARE_CENTS_AMT", "JS_DURN_SEC", "IMTT_DESC", "TAG1_DT_FK", "TAG2_DT_FK", "TAG1_TM", "TAG2_TM", "TAG1_TS_NM", "TAG1_LAT_VAL", "TAG1_LONG_VAL", "TAG2_TS_NM", "TAG2_LAT_VAL", "TAG2_LONG_VAL", "TAG1_OFF_PEAK_IND", "TAG2_OFF_PEAK_IND")

		// Define some terms for the processed data
		val w = Window.orderBy($"CARD_FK", $"TAG1_DT_FK", $"TAG1_TM".asc).partitionBy($"TAG1_DT_FK")
		val dateFormat = "yyyyMMdd HH:mm:ss"
		val opal = Array[String]("Senior/Pensioner", "Adult", "Free Travel", "School Student", "Child/Youth", "Employee", "Concession")
		val sundays = Array[Int](20160807, 20160814, 20160821, 20160828, 20160904, 20160911, 20160918)
		val saturdays = Array[Int](20160806, 20160813, 20160820, 20160827, 20160903, 20160910, 20160917)

		// Filter out totally unknown data
		val journey = dataSelected.filter($"TS_TYP_CD" =!= "UNKNOWN" && $"TAG1_DT_FK" =!= -1).withColumn("DISC_ENT_DESC", when($"DISC_ENT_DESC" === "No DE", regexp_replace(col("DISC_ENT_DESC"), "No DE", "No Discount")).otherwise($"DISC_ENT_DESC")).withColumn("CARD_TYP_CD", when($"DISC_ENT_DESC" === "No Discount", regexp_replace(col("CARD_TYP_CD"), "UNKNOWN", "Adult")).otherwise($"CARD_TYP_CD")).withColumn("TAG2_TM", when($"TAG2_DT_FK" === -1, $"TAG1_TM").otherwise($"TAG2_TM")).withColumn("TAG2_TS_NM", when($"TAG2_DT_FK" === -1, $"TAG1_TS_NM").otherwise($"TAG2_TS_NM")).withColumn("TAG2_LAT_VAL", when($"TAG2_DT_FK" === -1, $"TAG1_LAT_VAL").otherwise($"TAG2_LAT_VAL")).withColumn("TAG2_LONG_VAL", when($"TAG2_DT_FK" === -1, $"TAG1_LONG_VAL").otherwise($"TAG2_LONG_VAL")).withColumn("TAG2_OFF_PEAK_IND", when($"TAG2_DT_FK" === -1, $"TAG1_OFF_PEAK_IND").otherwise($"TAG2_OFF_PEAK_IND")).withColumn("TAG2_DT_FK", when($"TAG2_DT_FK" === -1, $"TAG1_DT_FK").when($"TAG2_DT_FK" =!= $"TAG1_DT_FK" && $"TAG1_DT_FK" === "20160731", $"TAG2_DT_FK").when($"TAG2_DT_FK" =!= $"TAG1_DT_FK" && $"TAG1_DT_FK" =!= "20160731", $"TAG1_DT_FK").otherwise($"TAG2_DT_FK")).withColumn("JS_DURN_SEC", when($"JS_DURN_SEC" < 0, abs(unix_timestamp(concat_ws(" ", $"TAG2_DT_FK", $"TAG2_TM"), dateFormat).cast("int") - unix_timestamp(concat_ws(" ", $"TAG1_DT_FK", $"TAG1_TM"), dateFormat).cast("int"))).otherwise($"JS_DURN_SEC")).withColumn("IMTT_DESC", when($"JS_DURN_SEC" > 0 && $"CARD_FK" === lag($"CARD_FK", 1).over(w) && $"TS_TYP_CD" =!= lag($"TS_TYP_CD", 1).over(w) && abs(unix_timestamp(concat_ws(" ", $"TAG1_DT_FK", $"TAG1_TM"), dateFormat).cast("int") - unix_timestamp(concat_ws(" ", lag($"TAG2_DT_FK",1).over(w), lag($"TAG2_TM", 1).over(w)), dateFormat).cast("int")) < 3600, concat(lag($"TS_TYP_CD", 1).over(w), lit(" to "), lower($"TS_TYP_CD"))).otherwise("No inter-modal transfer")).withColumn("IMTT_DESC", when($"IMTT_DESC" === "Bus to train", "Bus to rail").when($"IMTT_DESC" === "Ferry to train", "Ferry to rail").when($"IMTT_DESC" === "Light rail to train", "Light rail to rail").when($"IMTT_DESC" === "Train to ferry", "Rail to ferry").when($"IMTT_DESC" === "Train to bus", "Rail to bus").when($"IMTT_DESC" === "Train to light rail", "Rail to light rail").otherwise($"IMTT_DESC")).filter($"JS_DURN_SEC" > 0).withColumn("CARD_TYP_CD", when(not($"CARD_TYP_CD".isin(opal: _*)), "Sgl Trip Ticket").otherwise($"CARD_TYP_CD"))

		// Show total fare paid and total trips made during peak and offpeak hour for comparison
		journey.filter($"TAG1_OFF_PEAK_IND" === true).agg(count("JRNY_ID").as("TOTAL_OFF_PEAK_TRIPS"), (abs(sum("JS_DURN_SEC")/3600)).as("TOTAL_TRIP_TM"), (sum("JS_FARE_CENTS_AMT")).as("TOTAL_FARE_PAID")).show()
		journey.filter($"TAG1_OFF_PEAK_IND" === false).agg(count("JRNY_ID").as("TOTAL_PEAK_TRIPS"), (abs(sum("JS_DURN_SEC")/3600)).as("TOTAL_TRIP_TM"), (sum("JS_FARE_CENTS_AMT")).as("TOTAL_FARE_PAID")).show()

		// Extract information about station locations
		val deptstnAugSep = journey.groupBy("TAG1_TS_NM", "TAG1_LAT_VAL", "TAG1_LONG_VAL").count()

		val json1 = "{" + """"Info"""" + ": " + deptstnAugSep.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw1 = new PrintWriter(new File("departure.json"))
		pw1.write(json1)
		pw1.close()

		val deptOffpeak = journey.filter($"TAG1_OFF_PEAK_IND" === true).groupBy("TAG1_TS_NM", "TAG1_LAT_VAL", "TAG1_LONG_VAL").count()

		val json2 = "{" + """"Info"""" + ": " + deptOffpeak.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw2 = new PrintWriter(new File("departureOffpeak.json"))
		pw2.write(json2)
		pw2.close()

		val deptPeak = journey.filter($"TAG1_OFF_PEAK_IND" === false).groupBy("TAG1_TS_NM", "TAG1_LAT_VAL", "TAG1_LONG_VAL").count()

		val json3 = "{" + """"Info"""" + ": " + deptPeak.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw3 = new PrintWriter(new File("departurePeak.json"))
		pw3.write(json3)
		pw3.close()

		// Find destination stations in August and September
		val deststnAugSep = journey.groupBy("TAG2_TS_NM", "TAG2_LAT_VAL", "TAG2_LONG_VAL").count()

		val json4 = "{" + """"Info"""" + ": " + deststnAugSep.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw4 = new PrintWriter(new File("destination.json"))
		pw4.write(json4)
		pw4.close()

		val destOffpeak = journey.filter($"TAG2_OFF_PEAK_IND" === true).groupBy("TAG2_TS_NM", "TAG2_LAT_VAL", "TAG2_LONG_VAL").count()

		val json5 = "{" + """"Info"""" + ": " + destOffpeak.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw5 = new PrintWriter(new File("destinationOffpeak.json"))
		pw5.write(json5)
		pw5.close()

		val destPeak = journey.filter($"TAG2_OFF_PEAK_IND" === false).groupBy("TAG2_TS_NM", "TAG2_LAT_VAL", "TAG2_LONG_VAL").count()

		val json6 = "{" + """"Info"""" + ": " + destPeak.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw6 = new PrintWriter(new File("destinationPeak.json"))
		pw6.write(json6)
		pw6.close()

		// Extract information about departure stations based on card type
		val cardDeptAugSep = journey.groupBy("CARD_TYP_CD", "TAG1_TS_NM", "TAG1_LAT_VAL", "TAG1_LONG_VAL").count()

		val json7 = "{" + """"Info"""" + ": " + cardDeptAugSep.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw7 = new PrintWriter(new File("departureCard.json"))
		pw7.write(json7)
		pw7.close()

		val cardDeptOffpeak = journey.filter($"TAG1_OFF_PEAK_IND" === true).groupBy("CARD_TYP_CD", "TAG1_TS_NM", "TAG1_LAT_VAL", "TAG1_LONG_VAL").count()

		val json8 = "{" + """"Info"""" + ": " + cardDeptOffpeak.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw8 = new PrintWriter(new File("departureCardOffpeak.json"))
		pw8.write(json8)
		pw8.close()

		val cardDeptPeak = journey.filter($"TAG1_OFF_PEAK_IND" === false).groupBy("CARD_TYP_CD", "TAG1_TS_NM", "TAG1_LAT_VAL", "TAG1_LONG_VAL").count()

		val json9 = "{" + """"Info"""" + ": " + cardDeptPeak.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw9 = new PrintWriter(new File("departureCardPeak.json"))
		pw9.write(json9)
		pw9.close()

		// Extract information about destination stations based on card type
		val cardDestAugSep = journey.groupBy("CARD_TYP_CD", "TAG2_TS_NM", "TAG2_LAT_VAL", "TAG2_LONG_VAL").count()

		val json10 = "{" + """"Info"""" + ": " + cardDestAugSep.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw10 = new PrintWriter(new File("destinationCard.json"))
		pw10.write(json10)
		pw10.close()

		val cardDestOffpeak = journey.filter($"TAG2_OFF_PEAK_IND" === true).groupBy("CARD_TYP_CD", "TAG2_TS_NM", "TAG2_LAT_VAL", "TAG2_LONG_VAL").count()

		val json11 = "{" + """"Info"""" + ": " + cardDestOffpeak.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw11 = new PrintWriter(new File("destinationCardOffpeak.json"))
		pw11.write(json11)
		pw11.close()

		val cardDestPeak = journey.filter($"TAG2_OFF_PEAK_IND" === false).groupBy("CARD_TYP_CD", "TAG2_TS_NM", "TAG2_LAT_VAL", "TAG2_LONG_VAL").count()

		val json12 = "{" + """"Info"""" + ": " + cardDestPeak.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw12 = new PrintWriter(new File("destinationCardPeak.json"))
		pw12.write(json12)
		pw12.close()

		// Extract information about departure stations based on inter-modal options
		val intermodalDeptAugSep = journey.filter($"IMTT_DESC" =!= "No inter-modal transfer").groupBy("IMTT_DESC", "TAG1_TS_NM", "TAG1_LAT_VAL", "TAG1_LONG_VAL").count()

		val json13 = "{" + """"Info"""" + ": " + intermodalDeptAugSep.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw13 = new PrintWriter(new File("departureIntermodal.json"))
		pw13.write(json13)
		pw13.close()

		val intermodalDeptOffpeak = journey.filter($"IMTT_DESC" =!= "No inter-modal transfer" && $"TAG1_OFF_PEAK_IND" === true).groupBy("IMTT_DESC", "TAG1_TS_NM", "TAG1_LAT_VAL", "TAG1_LONG_VAL").count()

		val json14 = "{" + """"Info"""" + ": " + intermodalDeptOffpeak.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw14 = new PrintWriter(new File("departureIntermodalOffpeak.json"))
		pw14.write(json14)
		pw14.close()

		val intermodalDeptPeak = journey.filter($"IMTT_DESC" =!= "No inter-modal transfer" && $"TAG1_OFF_PEAK_IND" === false).groupBy("IMTT_DESC", "TAG1_TS_NM", "TAG1_LAT_VAL", "TAG1_LONG_VAL").count()

		val json15 = "{" + """"Info"""" + ": " + intermodalDeptPeak.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw15 = new PrintWriter(new File("departureIntermodalPeak.json"))
		pw15.write(json15)
		pw15.close()

		// Find general information
		val cardTypeIntermodal = journey.filter($"IMTT_DESC" =!= "No inter-modal transfer").groupBy("CARD_TYP_CD", "IMTT_DESC").count()

		val json16 = "{" + """"Info"""" + ": " + cardTypeIntermodal.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw16 = new PrintWriter(new File("cardTypeIntermodal.json"))
		pw16.write(json16)
		pw16.close()

		val passengerCardDiscount = journey.groupBy("PSNGR_TYP_CD", "CARD_TYP_CD", "DISC_ENT_DESC").count()

		val json17 = "{" + """"Info"""" + ": " + passengerCardDiscount.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw17 = new PrintWriter(new File("passengerCardDiscount.json"))
		pw17.write(json17)
		pw17.close()

		val passenger = journey.groupBy("PSNGR_TYP_CD").count()

		val json18 = "{" + """"Info"""" + ": " + passenger.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw18 = new PrintWriter(new File("passenger.json"))
		pw18.write(json18)
		pw18.close()

		val cardType = journey.groupBy("CARD_TYP_CD").count()

		val json19 = "{" + """"Info"""" + ": " + cardType.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw19 = new PrintWriter(new File("cardType.json"))
		pw19.write(json19)
		pw19.close()

		val transportMode = journey.groupBy("TS_TYP_CD").count()

		val json20 = "{" + """"Info"""" + ": " + transportMode.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw20 = new PrintWriter(new File("transportMode.json"))
		pw20.write(json20)
		pw20.close()

		// Find departure stations in Sundays and weekdays (including Saturdays)
		val departureSunday = journey.filter($"TAG1_DT_FK".isin(sundays: _*)).groupBy("TAG1_TS_NM", "TAG1_LAT_VAL", "TAG1_LONG_VAL").count()

		val json21 = "{" + """"Info"""" + ": " + departureSunday.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw21 = new PrintWriter(new File("departureSunday.json"))
		pw21.write(json21)
		pw21.close()

		val departureSaturday = journey.filter($"TAG1_DT_FK".isin(saturdays: _*)).groupBy("TAG1_TS_NM", "TAG1_LAT_VAL", "TAG1_LONG_VAL").count()

		val json22 = "{" + """"Info"""" + ": " + departureSaturday.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw22 = new PrintWriter(new File("departureSaturday.json"))
		pw22.write(json22)
		pw22.close()

		val departureWeekdays = journey.filter(not($"TAG1_DT_FK".isin(sundays: _*)) && not($"TAG1_DT_FK".isin(saturdays: _*))).groupBy("TAG1_TS_NM", "TAG1_LAT_VAL", "TAG1_LONG_VAL").count()

		val json23 = "{" + """"Info"""" + ": " + departureWeekdays.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw23 = new PrintWriter(new File("departureWeekday.json"))
		pw23.write(json23)
		pw23.close()

		// Find destination stations in Sundays and weekdays (including Saturdays)
		val destinationSunday = journey.filter($"TAG1_DT_FK".isin(sundays: _*)).groupBy("TAG2_TS_NM", "TAG2_LAT_VAL", "TAG2_LONG_VAL").count()

		val json24 = "{" + """"Info"""" + ": " + destinationSunday.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw24 = new PrintWriter(new File("destinationSunday.json"))
		pw24.write(json24)
		pw24.close()

		val destinationSaturday = journey.filter($"TAG1_DT_FK".isin(saturdays: _*)).groupBy("TAG2_TS_NM", "TAG2_LAT_VAL", "TAG2_LONG_VAL").count()

		val json25 = "{" + """"Info"""" + ": " + destinationSaturday.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw25 = new PrintWriter(new File("destinationSaturday.json"))
		pw25.write(json25)
		pw25.close()

		val destinationWeekday = journey.filter(not($"TAG1_DT_FK".isin(sundays: _*)) && not($"TAG1_DT_FK".isin(saturdays: _*))).groupBy("TAG2_TS_NM", "TAG2_LAT_VAL", "TAG2_LONG_VAL").count()

		val json26 = "{" + """"Info"""" + ": " + destinationWeekday.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw26 = new PrintWriter(new File("destinationWeekday.json"))
		pw26.write(json26)
		pw26.close()

		// Find card usage in for each card types Saturdays, Sundays and weekdays
		val cardSunday = journey.filter($"TAG1_DT_FK".isin(sundays: _*)).groupBy("PSNGR_TYP_CD", "CARD_TYP_CD").count()

		val json27 = "{" + """"Info"""" + ": " + cardSunday.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw27 = new PrintWriter(new File("cardSunday.json"))
		pw27.write(json27)
		pw27.close()

		val cardSaturday = journey.filter($"TAG1_DT_FK".isin(saturdays: _*)).groupBy("PSNGR_TYP_CD", "CARD_TYP_CD").count()

		val json28 = "{" + """"Info"""" + ": " + cardSaturday.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw28 = new PrintWriter(new File("cardSaturday.json"))
		pw28.write(json28)
		pw28.close()

		val cardWeekday = journey.filter(not($"TAG1_DT_FK".isin(sundays: _*)) && not($"TAG1_DT_FK".isin(saturdays: _*))).groupBy("PSNGR_TYP_CD", "CARD_TYP_CD").count()

		val json29 = "{" + """"Info"""" + ": " + cardWeekday.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw29 = new PrintWriter(new File("cardWeekday.json"))
		pw29.write(json29)
		pw29.close()

		// Calculate average time travel
		val averageDuration = journey.groupBy("TAG1_DT_FK").agg(avg("JS_DURN_SEC"))

		val json30 = "{" + """"Info"""" + ": " + averageDuration.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw30 = new PrintWriter(new File("averageDuration.json"))
		pw30.write(json30)
		pw30.close()

		val averageDurationSunday = journey.filter($"TAG1_DT_FK".isin(sundays: _*)).groupBy("TAG1_DT_FK").agg(avg("JS_DURN_SEC"))

		val json31 = "{" + """"Info"""" + ": " + averageDurationSunday.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw31 = new PrintWriter(new File("averageDurationSunday.json"))
		pw31.write(json31)
		pw31.close()

		val averageDurationSaturday = journey.filter($"TAG1_DT_FK".isin(saturdays: _*)).groupBy("TAG1_DT_FK").agg(avg("JS_DURN_SEC"))

		val json32 = "{" + """"Info"""" + ": " + averageDurationSaturday.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw32 = new PrintWriter(new File("averageDurationSaturday.json"))
		pw32.write(json32)
		pw32.close()

		val averageDurationWeekday = journey.filter(not($"TAG1_DT_FK".isin(sundays: _*)) && not($"TAG1_DT_FK".isin(saturdays: _*))).groupBy("TAG1_DT_FK").agg(avg("JS_DURN_SEC"))

		val json33 = "{" + """"Info"""" + ": " + averageDurationWeekday.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw33 = new PrintWriter(new File("averageDurationWeekday.json"))
		pw33.write(json33)
		pw33.close()

		// Find the cause of unknowns
		val unknowns = dataSelected.filter($"TS_TYP_CD" === "UNKNOWN" || $"TAG1_DT_FK" === -1).withColumn("DISC_ENT_DESC", when($"DISC_ENT_DESC" === "No DE", regexp_replace(col("DISC_ENT_DESC"), "No DE", "No Discount")).otherwise($"DISC_ENT_DESC")).withColumn("CARD_TYP_CD", when($"DISC_ENT_DESC" === "No Discount", regexp_replace(col("CARD_TYP_CD"), "UNKNOWN", "Adult")).when($"DISC_ENT_DESC" === "Concession", regexp_replace(col("CARD_TYP_CD"), "UNKNOWN", "Concession")).otherwise($"CARD_TYP_CD")).select("PSNGR_TYP_CD", "CARD_TYP_CD", "TAG2_TS_NM", "TAG2_LAT_VAL", "TAG2_LONG_VAL")

		val unknownStation = unknowns.groupBy("TAG2_TS_NM", "TAG2_LAT_VAL", "TAG2_LONG_VAL").count()

		val json34 = "{" + """"Info"""" + ": " + unknownStation.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw34 = new PrintWriter(new File("unknownStation.json"))
		pw34.write(json34)
		pw34.close()

		val unknownCard = unknowns.groupBy("PSNGR_TYP_CD", "CARD_TYP_CD").count();

		val json35 = "{" + """"Info"""" + ": " + unknownCard.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw35 = new PrintWriter(new File("unknownCard.json"))
		pw35.write(json35)
		pw35.close()

		val unknownStationWeekday = unknowns.filter(not($"TAG2_DT_FK".isin(sundays: _*)) && not($"TAG2_DT_FK".isin(saturdays: _*))).groupBy("TAG2_TS_NM", "TAG2_LAT_VAL", "TAG2_LONG_VAL").count()

		val json36 = "{" + """"Info"""" + ": " + unknownStationWeekday.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw36 = new PrintWriter(new File("unknownStationWeekday.json"))
		pw36.write(json36)
		pw36.close()

		val unknownStationSaturday = unknowns.filter($"TAG2_DT_FK".isin(saturdays: _*)).groupBy("TAG2_TS_NM", "TAG2_LAT_VAL", "TAG2_LONG_VAL").count()

		val json37 = "{" + """"Info"""" + ": " + unknownStationSaturday.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw37 = new PrintWriter(new File("unknownStationSaturday.json"))
		pw37.write(json37)
		pw37.close()

		val unknownStationSunday = unknowns.filter($"TAG2_DT_FK".isin(sundays: _*)).groupBy("TAG2_TS_NM", "TAG2_LAT_VAL", "TAG2_LONG_VAL").count()

		val json38 = "{" + """"Info"""" + ": " + unknownStationSunday.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw38 = new PrintWriter(new File("unknownStationSunday.json"))
		pw38.write(json38)
		pw38.close()

		val unknownCardWeekday = unknowns.filter(not($"TAG2_DT_FK".isin(sundays: _*)) && not($"TAG2_DT_FK".isin(saturdays: _*))).groupBy("PSNGR_TYP_CD", "CARD_TYP_CD").count()

		val json39 = "{" + """"Info"""" + ": " + unknownCardWeekday.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw39 = new PrintWriter(new File("unknownCardWeekday.json"))
		pw39.write(json39)
		pw39.close()

		val unknownCardSaturday = unknowns.filter($"TAG2_DT_FK".isin(saturdays: _*)).groupBy("PSNGR_TYP_CD", "CARD_TYP_CD").count()

		val json40 = "{" + """"Info"""" + ": " + unknownCardSaturday.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw40 = new PrintWriter(new File("unknownCardSaturday.json"))
		pw40.write(json40)
		pw40.close()

		val unknownCardSunday = unknowns.filter($"TAG2_DT_FK".isin(sundays: _*)).groupBy("PSNGR_TYP_CD", "CARD_TYP_CD").count()

		val json41 = "{" + """"Info"""" + ": " + unknownCardSunday.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw41 = new PrintWriter(new File("unknownCardSunday.json"))
		pw41.write(json41)
		pw41.close()

		// Find the time that users tap on the most
		val typeRoute = dataSelected.filter("ROUTE_val_ID != -1").groupBy("TS_TYP_CD", "ROUTE_val_NM").count()

		val json42 = "{" + """"Info"""" + ": " + typeRoute.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw42 = new PrintWriter(new File("typeRoute.json"))
		pw42.write(json42)
		pw42.close()

		val timeMode = journey.groupBy("TS_TYP_CD", "TAG1_TM").count()

		val json43 = "{" + """"Info"""" + ": " + timeMode.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw43 = new PrintWriter(new File("timeMode.json"))
		pw43.write(json43)
		pw43.close()

		val cardTimeMode = journey.groupBy("CARD_TYP_CD", "TS_TYP_CD", "TAG1_TM").count()

		val json44 = "{" + """"Info"""" + ": " + cardTimeMode.toJSON.collect().mkString("[", "," , "]") + "}"
		val pw44 = new PrintWriter(new File("cardTimeMode.json"))
		pw44.write(json44)
		pw44.close()

		val timeStation = journey.groupBy("TAG1_TS_NM", "TAG1_TM").count()

		timeStation.coalesce(1).write.format("csv").option("header", "true").option("delimiter", "|").save("timeStation.csv")

		val timeModeStation = journey.groupBy("TS_TYP_CD", "TAG1_TS_NM", "TAG1_TM").count()

		timeModeStation.coalesce(1).write.format("csv").option("header", "true").option("delimiter", "|").save("timeMode.csv")
	}
}
