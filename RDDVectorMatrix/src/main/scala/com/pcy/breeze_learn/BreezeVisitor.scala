package com.pcy.breeze_learn

import breeze.linalg.DenseVector
import org.apache.log4j.{Level, Logger}

/**
 * breeze 元素访问
 *
 * @ProductName: Hundsun HEP
 * @ProjectName: SparkMllibLearn
 * @Package: com.pcy.breeze_learn
 * @Description: note
 * @Author: pengcy31624
 * @CreateDate: 2020-08-03 13:15
 * @UpdateUser: pengcy31624
 * @UpdateDate: 2020-08-03 13:15
 * @UpdateRemark: The modified content
 * @Version: 1.0
 *
 *           Copyright © 2020 Hundsun Technologies Inc. All Rights Reserved
 **/

object BreezeVisitor {

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)

    val v1: DenseVector[Int] = DenseVector(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    // TODO: 此处元素的访问一直显示编译错误，后面进行修改
    val i: Int = v1.indexAt(1)
    println(i)

    println(v1.length)


  }

}
