title: 学习 Spark ML：朴素贝叶斯与数据分类
date: 2016-01-25 22:41:34
tags:
- Spark
- 机器学习
- 朴素贝叶斯
categories:
- 技术
---

这里的数据分类，是指给出一个字符串格式的数据，需要判断它是一个数字、日期、IP 地址等等，还是只是普通的文本，这样一个问题。本来这类问题有正则表达式这样的传统且有效的解决方案。不过实践过程中会发现，有时正则表达式的效率会有问题：特别是数据的类别很多的时候，最坏的情况可能会把所有的正则表达式都跑一边；即使采用多级正则匹配也会有严重影响相应时间的时候。

解决方案？简单的说还木有发现。不过本文在这里提供一个采用机器学习分类算法的数据分类实践，希望能起到抛砖引玉的作用。

> 如果你觉得 show me the code 更给力的话，本文使用的项目代码：[https://github.com/KaiserY/learn-spark-ml](https://github.com/KaiserY/learn-spark-ml)

## 选型

首先我们要解决的问题：

* 尽可能准确的识别数据的分类
* 同时能满足实时性要求（尽可能快且时间尽可能恒定）
* 同时支持尽可能多的数据类别
* 能提供 REST 接口，方便以微服务的形式与现有系统集成

这里我们选择了 Spark 的机器学习库 MLlib。算法则是经典的朴素贝叶斯（multinomial naive Bayes）。理由如下：

* 虽然朴素贝叶斯是个简单的算法，但实践中它的准确性却异常的好，足见统计规律的强大
* Spark 作为分布式内存计算框架，能提供一定的实时计算能力；同时朴素贝叶斯在分类算法中也算是灰常快的了，并且时间恒定
* 朴素贝叶斯支持非线性（多类别）分类
* Spark + Spray 可以轻松提供 REST 服务。这里甚至有现成的轮子 [PredictionIO](https://prediction.io/)

## 实现

因为 Spark 采用 Scala 编写，这里也采用 Scala 作为编程语言。

### 数据类别

以下是我们此次要识别的数据类别的枚举：

```scala
object DataType extends Enumeration {
  val TEXT = Value(0, "text")
  val INTEGER = Value("integer")
  val DOUBLE = Value("double")
  val EMAIL = Value("email")
  val FAKE_EMAIL = Value("fake email")
  val POST_CODE = Value("post code")
  val IP_V4 = Value("ipv4")
  val FAKE_IP_V4 = Value("fake ipv4")
  val UUID = Value("uuid")
  val DATE = Value("date")
  val TIME = Value("time")
}
```

关于`EMAIL`和`FAKE_EMAIL`：实践发现，朴素贝叶斯算法在分类特征明显的情况下准确率会大大提高。区分真邮箱与假邮箱是为了尽可能不把类似`abcd3f@12d`这样的并不是真正的邮箱的数据错分为邮箱。

### 训练数据

这里我们随机生成了大部分训练数据，像邮编、Email之类的则从文件读取。

```scala
case class DataWithLabel(label: Double, content: String)

object DataTypeSeq {

  val sampleCount = 10000

  def integerSeq(): Seq[DataWithLabel] = {
    Seq.fill(sampleCount) {
      val label = DataType.INTEGER.id.toDouble
      val content = scala.util.Random.nextInt().toString
      DataWithLabel(label, content)
    }
  }

  ...

  def emailSeq(): Seq[DataWithLabel] = {
    Source.fromInputStream(this.getClass.getResourceAsStream("/email.csv")).getLines().map(line => {
      val label = DataType.EMAIL.id.toDouble
      val content = line
      DataWithLabel(label, content)
    }).toSeq
  }

  ...

  def timeSeq_1(): Seq[DataWithLabel] = {
    Seq.fill(sampleCount) {
      val label = DataType.TIME.id.toDouble
      val content = new LocalTime(
        scala.util.Random.nextInt(23) + 1,
        scala.util.Random.nextInt(59) + 1,
        scala.util.Random.nextInt(59) + 1
      ).toString("hh时mm分ss秒")
      DataWithLabel(label, content)
    }
  }
}
```

之后我们我们生成`RDD`：

```scala
object DataTypeRDD {

  def makeRDD(sparkContext: SparkContext): RDD[DataWithLabel] = {

    val integerRDD = sparkContext.parallelize(DataTypeSeq.integerSeq())

    ...

    val timeRDD_1 = sparkContext.parallelize(DataTypeSeq.timeSeq_1())

    sparkContext.emptyRDD[DataWithLabel]
      .union(integerRDD)
      ...
      .union(timeRDD_1)
  }
}
```

Spark 1.6.0 之后提供了 Dataset API。不过支持程度不够完善，并不能对集合或自定义类型自动生成`encoder`，使用效果并没有`RDD`好，不过我们可以尝试一下：

```scala
object DataTypeDataset {

  def makeDataset(sqlContext: SQLContext): Dataset[DataWithLabel] = {

    import sqlContext.implicits._

    val integerDataset = sqlContext.createDataset(DataTypeSeq.integerSeq())

    ...

    val timeDataset_1 = sqlContext.createDataset(DataTypeSeq.timeSeq_1())

    integerDataset
      .union(integerDataset)
      ...
      .union(timeDataset_1)
  }
}
```

生成最终的`DataFrame`。这里我们用`75%`的样本数据进行训练，`25%`的数据用于测试。

```scala
val sparkConf = new SparkConf()
  .setAppName("Data Type Training")
  .setMaster("local")

val sparkContext = new SparkContext(sparkConf)

val sqlContext = new SQLContext(sparkContext)

// Dataset API
//    val trainingDataset = DataTypeDataset.makeDataset(sqlContext)
//
//    val dataFrame = trainingDataset.toDF().map(r => {
//      (r.getDouble(0), r.getString(1), Util.string2Vector(r.getString(1), 100))
//    }).toDF("label", "content", "features")

val trainingRDD = DataTypeRDD.makeRDD(sparkContext)

val dataFrame = sqlContext.createDataFrame(trainingRDD.map(d => {
  (d.label, d.content, Util.string2Vector(d.content, 100))
})).toDF("label", "content", "features")

val splitsDataFrame = dataFrame.randomSplit(Array(0.75, 0.25))

val trainingDataFrame = splitsDataFrame(0)
val testDataFrame = splitsDataFrame(1)
```

## 特征抽取

特征抽取函数如下：

```scala
object Util {

  def string2Vector(text: String, maxLength: Int): Vector = {

    val limit = if (text.length > maxLength) maxLength else text.length

    val maxCount = maxLength + 1

    val unicodePostionFeatureCount = 256 * 2 * maxLength

    val unicodeNumberFeatureCount = Features.maxId * maxCount

    var featureIndex, alphabet, number, dot, minus, at, colon = 0

    var featuresMap = new mutable.HashMap[Int, Double]

    val featureText = text.substring(0, limit)

    featureText.getBytes("Unicode").drop(2).foreach(byte => {
      val value = byte & 0xFF

      val index = featureIndex

      featureIndex += 256


      featuresMap += index + value -> 5.0
    })

    featureText.foreach({
      case char if '0' until '9' contains char => number += 1
      case char if 'a' until 'z' contains char => alphabet += 1
      case char if 'A' until 'Z' contains char => alphabet += 1
      case '.' => dot += 1
      case '-' => minus += 1
      case '@' => at += 1
      case ':' => colon += 1
      case _ =>
    })

    featureIndex = unicodePostionFeatureCount

    featuresMap += featureIndex + Features.NUMBER.id * maxCount + number -> 10.0
    featuresMap += featureIndex + Features.ALPHABET.id * maxCount + alphabet -> 10.0
    featuresMap += featureIndex + Features.DOT.id * maxCount + dot -> 20.0
    featuresMap += featureIndex + Features.MINUS.id * maxCount + minus -> 20.0
    featuresMap += featureIndex + Features.AT.id * maxCount + at -> 20.0
    featuresMap += featureIndex + Features.COLON.id * maxCount + colon -> 20.0

    Vectors.sparse(unicodePostionFeatureCount + unicodeNumberFeatureCount, featuresMap.toSeq)
  }
}
```

原理：我们首先取字符串数据的前 N 个字符，将其转化为 Unicode 字节数组（因为 Java 字符串转 Unicode 会带有 BOM，所以我们去掉头两个字节）。然后对应每个字节的值（`0`~`255`）给对应的特征赋值（这里我们根据实践选择赋值为`5.0`，可自行调整）。这样总共会有`N * 2 * 256`个特征向量。这样抽取特征有个缺点：Overfitting，因为现实中不同的字符提供的特征并不相同，这样对所有字符一视同仁会造成很大的噪音。为了解决这个问题，我们在语义层次加入了部分高权值的特征来平衡：对像字母，数字，加号，分号等有特殊意义的字符给予高于普通字符权值的特征。鉴于朴素贝叶斯算法对稀疏特征向量的支持良好，所以这个模型时可行的。

## 训练与测试

这里我们使用了 Spark MLlib 的 PipeLine。它对 Spark 的各种机器学习算法进行了抽象，统一了接口（`DataFrame`）。

这是我们使用 PipeLine 生成 Model 的函数：

```scala
def train(trainingDataFrame: DataFrame): PipelineModel = {
    val naiveBayes = new NaiveBayes()
      .setSmoothing(1.0)
      .setModelType("multinomial")
      .setLabelCol("label")
      .setFeaturesCol("features")
    val pipeline = new Pipeline()
      .setStages(Array(naiveBayes))

    pipeline.fit(trainingDataFrame)
}
```

这是我们的测试函数：

```scala
// test(testDataFrame)

def test(testDataFrame: DataFrame): Unit = {

  val predictions = model.transform(testDataFrame)

  val multiclassClassificationEvaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("precision")

  val accuracy = multiclassClassificationEvaluator.evaluate(predictions)

  val predictionAndLabel = predictions.select("prediction", "label").map(r => (r.getDouble(0), r.getDouble(1)))
  val metrics = new MulticlassMetrics(predictionAndLabel)

  val stringBuilder = new scala.collection.mutable.StringBuilder()

  stringBuilder.append("\n")
  stringBuilder.append(accuracy)
  stringBuilder.append("\n")
  stringBuilder.append(metrics.confusionMatrix.toString(10000, 10000))
  stringBuilder.append("\n")
  DataType.values.foreach(t => stringBuilder.append(t.toString.padTo(20, " ").mkString + "-->\t" + metrics.precision(t.id.toDouble) + "\n"))
  stringBuilder.append("\n")

  println(stringBuilder)
}
```

这里我们输出了总体的准确率，混淆矩阵和各数据类别的准确率：

```text
0.9997833100592286
7485.0  9.0     0.0     0.0     0.0      0.0    0.0     0.0     0.0     0.0     0.0     
0.0     7512.0  0.0     0.0     0.0      0.0    0.0     0.0     0.0     0.0     0.0     
0.0     0.0     9937.0  0.0     0.0      0.0    0.0     0.0     0.0     0.0     0.0     
0.0     0.0     0.0     9935.0  2.0      0.0    0.0     0.0     0.0     0.0     0.0     
0.0     0.0     0.0     0.0     12630.0  0.0    0.0     0.0     0.0     0.0     0.0     
0.0     1.0     0.0     0.0     0.0      657.0  0.0     0.0     0.0     0.0     0.0     
0.0     0.0     0.0     0.0     0.0      0.0    9983.0  5.0     0.0     0.0     0.0     
0.0     0.0     0.0     0.0     0.0      0.0    1.0     5038.0  0.0     0.0     0.0     
0.0     0.0     0.0     0.0     0.0      0.0    0.0     0.0     9970.0  0.0     0.0     
0.0     0.0     0.0     0.0     0.0      0.0    0.0     0.0     0.0     4996.0  0.0     
0.0     0.0     0.0     0.0     0.0      0.0    0.0     0.0     0.0     0.0     4907.0  
text                -->	1.0
integer             -->	0.9986705663387397
double              -->	1.0
email               -->	1.0
fake email          -->	0.9998416719442685
post code           -->	1.0
ipv4                -->	0.9998998397435898
fake ipv4           -->	0.9990085266706326
uuid                -->	1.0
date                -->	1.0
time                -->	1.0
```

## 结论

从上面的测试输出可以看出总体准确率还是不错的，不过在文本与整数，真邮箱与假邮箱和真 IP 与 假 IP 上的识别还存在问题。用个例测试的结论也是如此，究其原因应该是这几个分类的特征仍然没有很好的区分开。

在我们的机器上（i5，8G 内存）上的测试时间约为 1 分钟。并且目前 Spark 已支持将 Pipeline 保存到文件中。理论上应该能够进行实时计算。
