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
 * @CreateDate: 2020-08-04 17:14
 * @UpdateUser: pengcy31624
 * @UpdateDate: 2020-08-04 17:14
 * @UpdateRemark: The modified content
 * @Version: 1.0
 *
 *           Copyright © 2020 Hundsun Technologies Inc. All Rights Reserved
 **/

case class data(No: Int, year: Int, month: Int, day: Int, hour: Int,
                pm: Double, DEWP: Double, TEMP: Double, PRES: Double,
                cbwd: String, Iws: Double, Is: Double, Ir: Double)
