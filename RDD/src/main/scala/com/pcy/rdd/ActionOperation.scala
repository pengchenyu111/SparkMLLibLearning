package com.pcy.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD 行动操作
 *
 * @ProductName: Hundsun HEP
 * @ProjectName: SparkMllibLearn
 * @Package: com.pcy.rdd
 * @Description: note
 * @Author: pengcy31624
 * @CreateDate: 2020-08-03 9:14
 * @UpdateUser: pengcy31624
 * @UpdateDate: 2020-08-03 9:14
 * @UpdateRemark: The modified content
 * @Version: 1.0
 *
 *           Copyright © 2020 Hundsun Technologies Inc. All Rights Reserved
 **/

object ActionOperation {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("ActionOperation")
    val sparkContext = new SparkContext(sparkConf)

    // reduce(func)是对数据集的所有元素执行聚集(func)函数，该函数必须是可交换的。
    val arrayRDD1: RDD[Int] = sparkContext.parallelize(Array(1, 2, 3, 4, 5))
    val ans1: Int = arrayRDD1.reduce(_ + _)
    println("********************")
    println(ans1)

    // collect是将数据集中的所有元素以一个array的形式返回。
    val ans2: String = arrayRDD1.collect().mkString(",")
    println("********************")
    println(ans2)

    // count返回数据集中元素的个数
    val ans3: Long = arrayRDD1.count()
    println("********************")
    println(ans3)

    // first返回数据集中的第一个元素， 类似于take(1)
    val ans4: Int = arrayRDD1.first()
    println("********************")
    println(ans4)

    // Take(n)返回一个包含数据集中前n个元素的数组， 当前该操作不能并行
    val ans5: Array[Int] = arrayRDD1.take(2)
    println("********************")
    println(ans5)

    // takeSample(withReplacement,num, [seed])返回包含随机的num个元素的数组
    // 和Sample不同，takeSample 是行动操作，所以返回 的是数组而不是RDD
    // 其中第一个参数withReplacement是抽样时是否放回，第二个参数num会精确指定抽样数，而不是比例
    val ans6: Array[Int] = arrayRDD1.takeSample(true, 3, 0)
    println("********************")
    println(ans6)

    // takeOrdered(n， [ordering])是返回包含随机的n个元素的数组，按照顺序输出
    val arrayRDD2: RDD[Int] = sparkContext.parallelize(Array(164, 2, 85, 23, 46, 32, 21, 65, 346))
    val ans7: Array[Int] = arrayRDD2.takeOrdered(4)
    println("********************")
    println(ans7)

    // saveAsTextFile 把数据集中的元素写到一个文本文件，Spark会对每个元素调用toString方法来把每个元素存成文本文件的一行
    arrayRDD1.saveAsTextFile("D:\\MyOwnCodes\\IJIDEAJAVA\\SparkMllibLearn\\RDD\\src\\main\\resources\\data1.txt")

    // countByKey 对于(K, V)类型的RDD. 返回一个(K, Int)的map， Int为K的个数
    val kvRDD: RDD[(Int, Int)] = sparkContext.parallelize(Array((1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (2, 3)), 3)
    val ans8: collection.Map[Int, Long] = kvRDD.countByKey()
    println("********************")
    println(ans8)

    // foreach(func)是对数据集中的每个元素都执行func函数
    arrayRDD1.foreach(x => x + 1)
    println("********************")
    println(arrayRDD1.collect().mkString(","))


  }

}
