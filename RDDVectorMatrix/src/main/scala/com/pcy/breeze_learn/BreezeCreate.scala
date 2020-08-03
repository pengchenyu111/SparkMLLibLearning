package com.pcy.breeze_learn

import breeze.linalg.{DenseMatrix, DenseVector, Transpose, diag}
import org.apache.log4j.{Level, Logger}

/**
 * 文件描述
 *
 * @ProductName: Hundsun HEP
 * @ProjectName: SparkMllibLearn
 * @Package: com.pcy.breeze
 * @Description: note
 * @Author: pengcy31624
 * @CreateDate: 2020-08-03 10:55
 * @UpdateUser: pengcy31624
 * @UpdateDate: 2020-08-03 10:55
 * @UpdateRemark: The modified content
 * @Version: 1.0
 *
 *           Copyright © 2020 Hundsun Technologies Inc. All Rights Reserved
 **/

object BreezeCreate {
  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)


    // 全0向量
    val v1: DenseVector[Double] = DenseVector.zeros[Double](3)
    println("v1" + "****************************")
    println(v1)

    // 全1向量
    val v2: DenseVector[Double] = DenseVector.ones[Double](3)
    println("v2" + "****************************")
    println(v2)

    // 按数值填充向量
    val v3: DenseVector[Double] = DenseVector.fill(3) {
      5.0
    }
    println("v3" + "****************************")
    println(v3)

    // 生成随机向量
    val v4: DenseVector[Int] = DenseVector.range(1, 10, 2)
    println("v4" + "****************************")
    println(v4)

    // 按行创建向量
    val v5: DenseVector[Int] = DenseVector(1, 2, 3, 4)
    println("v5" + "****************************")
    println(v5)

    // 向量转置
    val v6: Transpose[DenseVector[Int]] = DenseVector(1, 2, 3, 4).t
    println("v6" + "****************************")
    println(v6)

    // 函数创建向量
    val v7: DenseVector[Int] = DenseVector.tabulate(3) { i => 2 * i }
    println("v7" + "****************************")
    println(v7)

    // 数组创建向量
    val v8 = new DenseVector(Array(1, 2, 3, 4))
    println("v8" + "****************************")
    println(v8)

    // 0到 1的随机向量
    val v9 = DenseVector.rand(4)
    println("v9" + "****************************")
    println(v9)


    // 全0矩阵
    val m1: DenseMatrix[Double] = DenseMatrix.zeros[Double](2, 3)
    println("m1" + "****************************")
    println(m1)

    // 单位矩阵
    val m2: DenseMatrix[Double] = DenseMatrix.eye[Double](3)
    println("m2" + "****************************")
    println(m2)

    // 对角矩阵
    val m3: DenseMatrix[Double] = diag(DenseVector(1.0, 2.0, 3.0))
    println("m3" + "****************************")
    println(m3)

    // 函数创建矩阵
    val m4: DenseMatrix[Int] = DenseMatrix.tabulate(3, 2) { case (i, j) => i + j }
    println("m4" + "****************************")
    println(m4)

    // 数组创建矩阵
    val m5 = new DenseMatrix(2, 3, Array(11, 12, 13, 21, 22, 23))
    println("m5" + "****************************")
    println(m5)

    // 从0到1的随机矩阵
    val m6 = DenseMatrix.rand(2, 3)
    println("m6" + "****************************")
    println(m6)
  }
}
