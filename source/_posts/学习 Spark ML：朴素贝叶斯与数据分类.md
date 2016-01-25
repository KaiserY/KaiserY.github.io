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
      ).toString("hh时mm分ss庙秒")
      DataWithLabel(label, content)
    }
  }
}
```

之后
