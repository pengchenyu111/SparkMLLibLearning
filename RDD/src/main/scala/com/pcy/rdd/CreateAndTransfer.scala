package com.pcy.rdd


import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 文件描述
 *
 * @ProductName: Hundsun HEP
 * @ProjectName: SparkMllibLearn
 * @Package: com.pcy.rdd
 * @Description: note
 * @Author: pengcy31624
 * @CreateDate: 2020-07-31 15:16
 * @UpdateUser: pengcy31624
 * @UpdateDate: 2020-07-31 15:16
 * @UpdateRemark: The modified content
 * @Version: 1.0
 *
 *           Copyright © 2020 Hundsun Technologies Inc. All Rights Reserved
 **/

object CreateAndTransfer {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("RDDCreate").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)

    Logger.getRootLogger.setLevel(Level.WARN)

    // 用数组创建RDD,
    // parallelize方法第一个参数是一个 Seq集合
    // 第二个参数是分区数
    // 返回的是RDD[T]
    val array = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val arrayRDD: RDD[Int] = sparkContext.parallelize(array, 3)
    val res0: Array[Int] = arrayRDD.collect()
    res0.foreach(println)

    // 读取外部文件
    val fileRDD: RDD[String] = sparkContext.textFile("D:\\MyOwnCodes\\IJIDEAJAVA\\SparkMllibLearn\\LinearRession\\src\\main\\resources\\murder\\factors.txt")

    // RDD转换操作
    // map
    val transferRDD1: RDD[Int] = arrayRDD.map(x => x + 1)
    println("********************************")
    transferRDD1.collect().foreach(println)

    // filter
    val transferRDD2: RDD[Int] = arrayRDD.filter(x => x % 2 != 0)
    println("********************************")
    transferRDD2.collect().foreach(println)

    // flatMap类似于map，但是每一个输入元素，会被映射为0到多个输出元素（因此，func函数的返回值是一个Seq，而不是单一元素）， RDD之间的元素是一对多关系；
    val transferRDD3: RDD[Int] = arrayRDD.flatMap(x => Array(6, 6, 6))
    println("********************************")
    transferRDD3.collect().foreach(println)

    // sample(withReplacement,fraction,seed)
    // 根据给定的随机种子seed
    // 随机抽样出数量为frac的数据,当withReplacement=false时：每个元素被选中的概率;fraction 必须是[0,1], 当withReplacement=true时：每个元素被选中的期望; fraction 必须大于等于0
    // withReplacement：是否放回抽 样；
    // fraction：比例，0.1表示10% ；
    val arrayRDD2: RDD[Int] = sparkContext.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3)
    val sampleRDD: RDD[Int] = arrayRDD2.sample(true, 0.5, 1)
    println("********************************")
    println(sampleRDD.collect().mkString(","))

    // union(otherDataset)是数据合并，返回一个新的数据集，由原数据集和otherDataset联合而成
    val array1 = Array(1, 2, 3)
    val arrayRDD4: RDD[Int] = sparkContext.parallelize(array1)
    val unionRDD: RDD[Int] = arrayRDD.union(arrayRDD4)
    println("********************************")
    println(unionRDD.collect().mkString(", "))

    // intersection(otherDataset)是数据交集，返回一个新的数据集，包含两个数据集的交集数据；
    val intersectionRDD: RDD[Int] = arrayRDD.intersection(arrayRDD4)
    println("********************************")
    println(intersectionRDD.collect().mkString(", "))

    // distinct([numTasks]))是数据去重，返回一个数据集，是对两个数据集去除重复数据，numTasks参数是设置任务并行数量。
    val array2 = Array(1, 1, 2, 2)
    val arrayRDD5: RDD[Int] = sparkContext.parallelize(array2)
    val distinctRDD: RDD[Int] = arrayRDD5.distinct()
    println("********************************")
    println(distinctRDD.collect().mkString(","))

    // groupByKey([numTasks])是数据分组操作，在一个由（K,V）对组成的数据集上调用，返回一个（K,Seq[V])对的数据集。
    val kvRDD: RDD[(Int, Int)] = sparkContext.parallelize(Array((1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (2, 3)), 3)
    val groupByKeyRDD: RDD[(Int, Iterable[Int])] = kvRDD.groupByKey()
    println("********************************")
    println(groupByKeyRDD.collect().mkString(","))

    // reduceByKey(func, [numTasks])是数据分组聚合操作，在一个（K,V)对的数据集上使用，返回一个（K,V）对的数据集，key相同的值， 都被使用指定的reduce函数聚合到一起
    val reduceByKeyRDD: RDD[(Int, Int)] = kvRDD.reduceByKey((x, y) => x * y)
    println("********************************")
    println(reduceByKeyRDD.collect().mkString(","))

    // sortByKey([ascending],[numTasks])是排序操作，对(K,V)类型的数据按照K进行排序，其中K需要实现Ordered方法。
    val sortByKeyRDD: RDD[(Int, Int)] = kvRDD.sortByKey()
    println("********************************")
    println(sortByKeyRDD.collect().mkString(","))

    // join(otherDataset, [numTasks])是连接操作，将输入数据集(K,V)和另外一个数据集(K,W)进行Join， 得到(K, (V,W))；该操作是对于相同K 的V和W集合进行笛卡尔积 操作，也即V和W的所有组合；
    // 连接操作除join 外，还有左连接、右连接、全连接操作函数： leftOuterJoin、rightOuterJoin、fullOuterJoin
    val joinRDD: RDD[(Int, (Int, Int))] = kvRDD.join(kvRDD)
    println("********************************")
    println(joinRDD.collect().mkString(","))

    // cogroup(otherDataset, [numTasks])是将输入数据集(K, V)和另外一个数据集(K, W)进行cogroup，得到一个格式为(K, Seq[V], Seq[W]) 的数据集。
    val cogroupRDD: RDD[(Int, (Iterable[Int], Iterable[Int]))] = kvRDD.cogroup(kvRDD)
    println("********************************")
    println(cogroupRDD.collect().mkString(","))

    // cartesian(otherDataset)是做笛卡尔积：对于数据集T和U 进行笛卡尔积操作， 得到(T, U)格式的数据集。
    val cartesianRDD: RDD[(Int, Int)] = arrayRDD.cartesian(arrayRDD2)
    println("********************************")
    println(cartesianRDD.collect().mkString(","))

  }
}
