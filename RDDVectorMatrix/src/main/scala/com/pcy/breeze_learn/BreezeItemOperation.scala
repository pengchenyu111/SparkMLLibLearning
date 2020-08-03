package com.pcy.breeze_learn

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 元素操作
 *
 * @ProductName: Hundsun HEP
 * @ProjectName: SparkMllibLearn
 * @Package: com.pcy.breeze_learn
 * @Description: note
 * @Author: pengcy31624
 * @CreateDate: 2020-08-03 13:44
 * @UpdateUser: pengcy31624
 * @UpdateDate: 2020-08-03 13:44
 * @UpdateRemark: The modified content
 * @Version: 1.0
 *
 *           Copyright © 2020 Hundsun Technologies Inc. All Rights Reserved
 **/

object BreezeItemOperation {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("BreezeItemOperation")
    val sparkContext = new SparkContext(sparkConf)

    Logger.getRootLogger.setLevel(Level.WARN)


    val m: DenseMatrix[Double] = DenseMatrix((1.0, 2.0, 3.0), (3.0, 4.0, 5.0))
    val m2: DenseMatrix[Double] = DenseMatrix((1.0, 2.0, 3.0), (3.0, 4.0, 5.0), (7.0, 8.0, 9.0))
    // 调整矩阵形状
    val m1: DenseMatrix[Double] = m.reshape(3, 2)
    println("m1********************")
    println(m1)

    // 矩阵转向量
    val v1: DenseVector[Double] = m.toDenseVector
    println("v1********************")
    println(v1)

    // 矩阵复制
    val m3: DenseMatrix[Double] = m2.copy
    println("m3********************")
    println(m3)


    // 矩阵列的所有值赋值
    val v2: DenseVector[Double] = m(::, 2) := 5.0
    println("v2********************")
    println(v2)
    println(m)

    // 矩阵赋值
    val m4: DenseMatrix[Double] = m2(1 to 2, 1 to 2) := 11.0
    println("m4********************")
    println(m4)


    val v3: DenseVector[Int] = DenseVector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    // 向量元素获取
    val v4: DenseVector[Int] = v3(1 to 4)
    println("v4********************")
    println(v4)

    // 向量部分改变
    val v5: DenseVector[Int] = v3(1 to 4) := 5
    println("v5********************")
    println(v5)
    println(v3)
    v3(1 to 4) := DenseVector(1, 2, 3, 4)
    println(v3)


  }

}
