package com.pcy.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 文件描述
 *
 * @ProductName: Hundsun HEP
 * @ProjectName: SparkMllibLearn
 * @Package: com.pcy.example
 * @Description: note
 * @Author: pengcy31624
 * @CreateDate: 2020-08-04 17:28
 * @UpdateUser: pengcy31624
 * @UpdateDate: 2020-08-04 17:28
 * @UpdateRemark: The modified content
 * @Version: 1.0
 *
 *           Copyright © 2020 Hundsun Technologies Inc. All Rights Reserved
 **/

object PMPrediction {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("foreast")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    Logger.getLogger("org").setLevel(Level.ERROR)

    import sqlContext.implicits._

    val df = sc.textFile("D:\\MyOwnCodes\\IJIDEAJAVA\\SparkMllibLearn\\RandomForeast\\src\\main\\resources\\example\\pm.csv")
      .map(_.split(","))
      .filter(_ (0) != "No")
      .filter(!_.contains("NaN"))
      .map(
        x => data(
          x(0).toInt,
          x(1).toInt,
          x(2).toInt,
          x(3).toInt,
          x(4).toInt,
          x(5).toDouble,
          x(6).toDouble,
          x(7).toDouble,
          x(8).toDouble,
          x(9),
          x(10).toDouble,
          x(11).toDouble,
          x(12).toDouble)
      ).toDF.drop("No").drop("year")


    val splitdf = df.randomSplit(Array(0.8, 0.2))
    val (train, test) = (splitdf(0), splitdf(1))
    val traindf = train.withColumnRenamed("pm", "label")

    val indexer = new StringIndexer().setInputCol("cbwd").setOutputCol("cbwd_")
    val assembler = new VectorAssembler().setInputCols(Array("month", "day", "hour", "DEWP", "TEMP", "PRES", "cbwd_", "Iws", "Is", "Ir")).setOutputCol("features")

    import org.apache.spark.ml.Pipeline

    val rf = new RandomForestRegressor().setLabelCol("label").setFeaturesCol("features")
    // setMaxDepth最大20，会大幅增加计算时间，增大能有效减小根均方误差
    // setMaxBins我觉得和数据量相关，单独增大适得其反，要和setNumTrees一起增大
    // 目前这个参数得到的评估结果（根均方误差）46.0002637676162
    // val numClasses=2
    // val categoricalFeaturesInfo = Map[Int, Int]()// Empty categoricalFeaturesInfo indicates all features are continuous.

    val pipeline = new Pipeline().setStages(Array(indexer, assembler, rf))
    val model = pipeline.fit(traindf)

    val testdf = test.withColumnRenamed("pm", "label")
    val labelsAndPredictions = model.transform(testdf)

    labelsAndPredictions.select("prediction", "label", "features").show(false)

    val eva = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction")
    val accuracy = eva.evaluate(labelsAndPredictions)
    println("accuracy: " + accuracy)

    val treemodel = model.stages(2).asInstanceOf[RandomForestRegressionModel]
    println("Learned regression forest model:\n" + treemodel.toDebugString)
  }

}
