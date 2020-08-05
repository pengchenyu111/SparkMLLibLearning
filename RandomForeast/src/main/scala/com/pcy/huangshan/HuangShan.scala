package com.pcy.huangshan

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import com.pcy.huangshan.HuangShanData
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}

/**
 * 文件描述
 *
 * @ProductName: Hundsun HEP
 * @ProjectName: SparkMllibLearn
 * @Package: com.pcy.huangshan
 * @Description: note
 * @Author: pengcy31624
 * @CreateDate: 2020-08-04 22:53
 * @UpdateUser: pengcy31624
 * @UpdateDate: 2020-08-04 22:53
 * @UpdateRemark: The modified content
 * @Version: 1.0
 *
 *           Copyright © 2020 Hundsun Technologies Inc. All Rights Reserved
 **/

object HuangShan {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("forecast")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    Logger.getRootLogger.setLevel(Level.ERROR)

    import sqlContext.implicits._

    val df = sc.textFile("D:\\MyOwnCodes\\IJIDEAJAVA\\SparkMllibLearn\\RandomForeast\\src\\main\\resources\\huangshan\\factors.csv")
      .map(_.split(","))
      .filter(_ (0) != "totalNum")
      .map(
        x => HuangShanData(
          x(0).toInt,
          x(1).toInt,
          x(2).toInt,
          x(3).toInt,
          x(4).toInt,
          x(5).toInt,
          x(6).toInt,
          x(7).toInt,
          x(8).toInt,
          x(9).toInt,
          x(10).toInt)
      ).toDF

    val splitDF: Array[Dataset[Row]] = df.randomSplit(Array(0.8, 0.2))
    val (train, test) = (splitDF(0), splitDF(1))
    val trainDF: DataFrame = train.withColumnRenamed("totalNum", "label")

    val assembler = new VectorAssembler().setInputCols(
      Array("order", "pre1", "pre2", "pre3", "pre7", "weather", "holidayType", "weekType", "classifier", "weekday")
    ).setOutputCol("features")

    import org.apache.spark.ml.Pipeline

    val rf = new RandomForestRegressor().setLabelCol("label").setFeaturesCol("features")
    val pipeline = new Pipeline().setStages(Array(assembler, rf))
    val model = pipeline.fit(trainDF)

    val testDF = test.withColumnRenamed("totalNum", "label")
    val labelsAndPredictions = model.transform(testDF)

    labelsAndPredictions.select("prediction", "label", "features").show(false)

    val eva = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction")
    val accuracy = eva.evaluate(labelsAndPredictions)
    println("accuracy: " + accuracy)

    val treemodel = model.stages(1).asInstanceOf[RandomForestRegressionModel]
    println("Learned regression forest model:\n" + treemodel.toDebugString)

  }


}
