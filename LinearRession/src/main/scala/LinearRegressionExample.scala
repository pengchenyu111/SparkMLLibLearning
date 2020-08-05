
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

/**
 * 文件描述
 *
 * @ProductName: Hundsun HEP
 * @ProjectName: SparkMllibLearn
 * @Package:
 * @Description: note
 * @Author: pengcy31624
 * @CreateDate: 2020-08-03 15:13
 * @UpdateUser: pengcy31624
 * @UpdateDate: 2020-08-03 15:13
 * @UpdateRemark: The modified content
 * @Version: 1.0
 *
 *           Copyright © 2020 Hundsun Technologies Inc. All Rights Reserved
 **/

object LinearRegressionExample {

  def main(args: Array[String]) {

    // 构建Spark对象
    val conf = new SparkConf().setAppName("LinearRegressionWithSGD")
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    // 获取脚本数据
    val data_path1 = "D:\\MyOwnCodes\\IJIDEAJAVA\\SparkMllibLearn\\LinearRession\\src\\main\\resources\\lpsa.data"
    val data = sc.textFile(data_path1)
    // 这里有必要做下归一化，有利于模型的准确
    val examples = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache()
    val numExamples = examples.count()

    // 新建线性回归模型，并设置训练参数
    // 迭代次数
    val numIterations = 100
    // 迭代步长
    val stepSize = 1
    // 随机抽样比率
    val miniBatchFraction = 1.0
    val model = LinearRegressionWithSGD.train(examples, numIterations, stepSize, miniBatchFraction)
    // 各个系数
    model.weights
    // 偏置
    model.intercept

    // 对样本进行测试
    val prediction = model.predict(examples.map(_.features))
    val predictionAndLabel = prediction.zip(examples.map(_.label))
    val print_predict = predictionAndLabel.take(20)
    println("prediction" + "\t" + "label")
    for (i <- 0 to print_predict.length - 1) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }
    // 计算误差：均方根误差
    val loss = predictionAndLabel.map {
      case (p, l) =>
        val err = p - l
        err * err
    }.reduce(_ + _)
    val rmse = math.sqrt(loss / numExamples)
    println(s"Test RMSE = $rmse.")

    // 保存模型
    val ModelPath = "/user/huangmeiling/LinearRegressionModel"
    model.save(sc, ModelPath)

    val sameModel = LinearRegressionModel.load(sc, ModelPath)

  }

}
