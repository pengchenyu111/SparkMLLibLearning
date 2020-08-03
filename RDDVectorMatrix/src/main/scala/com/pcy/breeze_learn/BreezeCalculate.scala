package com.pcy.breeze_learn

import breeze.linalg.DenseMatrix
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Breeze计算
 *
 * @ProductName: Hundsun HEP
 * @ProjectName: SparkMllibLearn
 * @Package: com.pcy.breeze_learn
 * @Description: note
 * @Author: pengcy31624
 * @CreateDate: 2020-08-03 14:12
 * @UpdateUser: pengcy31624
 * @UpdateDate: 2020-08-03 14:12
 * @UpdateRemark: The modified content
 * @Version: 1.0
 *
 *           Copyright © 2020 Hundsun Technologies Inc. All Rights Reserved
 **/

object BreezeCalculate {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("BreezeCalculate")
    val sparkContext = new SparkContext(sparkConf)

    Logger.getRootLogger.setLevel(Level.WARN)

    val a = DenseMatrix((1.0, 2.0, 3.0), (4.0, 5.0, 6.0))
    val b = DenseMatrix((1.0, 1.0, 1.0), (2.0, 2.0, 2.0))

    // 元素加法
    val m1: DenseMatrix[Double] = a + b
    println("m1" + "************************")
    println(m1)

  }
}
