package edu.knoldus.model


import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

case class FileOperator() {

  val sparkContext = new SparkContext("local", "fileOperations")

  val customerRDD: RDD[Array[String]] = sparkContext.textFile(CUSTOMER_LOCATION)
    .map { x => x.split('#') }
  val salesRDD: RDD[Array[String]] = sparkContext.textFile(SALES_LOCATION)
    .map { x => x.split('#') }
  val salesFormatted: RDD[(Int, Int, Int, Long, Long)] = salesRDD.map {
    array => {
      val dateTime = new DateTime(array(0).toLong * 1000L)
      (dateTime.getYear, dateTime.getMonthOfYear, dateTime.getDayOfMonth, array(1).toLong, array(2).toLong)
    }
  }
  val customerFormatted: RDD[(Long, String)] = customerRDD.map {
    array => {
      (array(0).toLong, array(3))
    }
  }

  def getTotalSales: RDD[(Long, (String, String))] = {

    val yearlySalesData = salesFormatted.groupBy {
      tuple => (tuple._1, tuple._4)
    }
    val monthlySalesData = salesFormatted.groupBy {
      tuple => (tuple._1, tuple._2, tuple._4)
    }
    val dailySalesData = salesFormatted.groupBy {
      tuple => (tuple._1, tuple._2, tuple._3, tuple._4)
    }

    val yearlySum = yearlySalesData.map {
      oneTuple =>
        (oneTuple._1._2, oneTuple._1._1, "#", "#",
          oneTuple._2.foldLeft(0.toLong)((accumulate, tuple) => accumulate + tuple._5))
    }
    val monthlySum = monthlySalesData.map {
      oneTuple =>
        (oneTuple._1._3, oneTuple._1._1, oneTuple._1._2, "#",
          oneTuple._2.foldLeft(0.toLong)((accumulate, tuple) => accumulate + tuple._5))
    }
    val dailySum = dailySalesData.map {
      oneTuple =>
        (oneTuple._1._4, oneTuple._1._1, oneTuple._1._2, oneTuple._1._3,
          oneTuple._2.foldLeft(0.toLong)((accumulate, tuple) => accumulate + tuple._5))
    }

    val yearlyResult = yearlySum.map {
      tuple => (tuple._1, s"#${tuple._2}#${tuple._3}#${tuple._4}#${tuple._5}")
    }

    val monthlyResult = monthlySum.map {
      tuple => (tuple._1, s"#${tuple._2}#${tuple._3}#${tuple._4}#${tuple._5}")
    }

    val dailyResult = dailySum.map {
      tuple => (tuple._1, s"#${tuple._2}#${tuple._3}#${tuple._4}#${tuple._5}")
    }

    val yearlyJoinedData = customerFormatted.join(yearlyResult)
    val monthlyJoinedData = customerFormatted.join(monthlyResult)
    val dailyJoinedResult = customerFormatted.join(dailyResult)
    yearlyJoinedData ++ monthlyJoinedData ++ dailyJoinedResult
  }

  def stringifyResult(joinedData: RDD[(Long, (String, String))]): Unit = {
    val result = joinedData.map {
      data => data._2._1 + data._2._2
    }.sortBy(x => x)
    result.repartition(1).saveAsTextFile("documents/result")
  }

}
