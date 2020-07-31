import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 文件描述
 *
 * @ProductName: Hundsun HEP
 * @ProjectName: SparkMllibLearn
 * @Package:
 * @Description: note
 * @Author: pengcy31624
 * @CreateDate: 2020-07-27 19:57
 * @UpdateUser: pengcy31624
 * @UpdateDate: 2020-07-27 19:57
 * @UpdateRemark: The modified content
 * @Version: 1.0
 *
 *           Copyright © 2020 Hundsun Technologies Inc. All Rights Reserved
 **/

object TravelData {

  case class Schema(label: Double, features: org.apache.spark.ml.linalg.Vector)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("travelData")
      .master("local")
      .getOrCreate()
    val sparkContext = spark.sparkContext

    val rdd = sparkContext.textFile("D:\\MyOwnCodes\\IJIDEAJAVA\\SparkMllibLearn\\LinearRession\\src\\main\\resources\\murder\\factors.txt")

    import spark.implicits._

    val training = rdd.map {
      line =>
        val splits = line.split("\t")
        Schema(
          splits(0).toDouble,
          Vectors.dense(Array(splits(1), splits(2), splits(3), splits(4), splits(5), splits(6), splits(7), splits(8), splits(9)).map(_.toDouble)))
    }.toDF

    val lr = new LinearRegression().setMaxIter(100).setRegParam(0.3).setElasticNetParam(0.8)

    val lrModule = lr.fit(training)


    println(s"常数b： ${lrModule.intercept}")
    println(s"偏回归系数： ${lrModule.coefficients}")
    println(s"判定系数： ${lrModule.summary.r2}")
    println(s"均方根误差： ${lrModule.summary.rootMeanSquaredError}")
    println(s"平均绝对误差： ${lrModule.summary.meanAbsoluteError}")

    lrModule.summary.predictions.show()
  }
}
