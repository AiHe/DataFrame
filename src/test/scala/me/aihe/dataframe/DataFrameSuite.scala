package me.aihe.dataframe

import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by aihe on 12/21/15.
  */
class DataFrameSuite extends FunSuite with BeforeAndAfterAll {

  private var df: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    df = DataFrame.loadCSV(path = "data/sample.csv", tableName = "fraud", header = true)
  }

  test("load csv") {
    assert(df.length == 36634)
    assert(df.size == 36634)
    assert(df.width == 18)
  }

  test("for expression") {
    val rows = for {
      r <- df
      pid <- r.getAs[Int]("policyID")
      if pid.toString.startsWith("1111")
    } yield r
    val newDF = DataFrame.fromRows(df, rows)
    println(newDF.headOption)
    assert(newDF.forall(_.getAs[Int]("policyID").exists(_.toString.startsWith("1111"))))
  }

  test("partition") {
    val (df1, df2) = df.partition(r => r.getAs[Int]("policyID").exists(_.toString.startsWith("1")))
    println(df1.headOption)
    println(df2.headOption)
    println(df1.size)
    println(df2.length)
    assert(df1.forall(_.getAs[Int]("policyID").exists(_.toString.startsWith("1"))))
    assert(df2.forall(!_.getAs[Int]("policyID").exists(_.toString.startsWith("1"))))
    assert(df.length == 36634)
  }

  test("take") {
    val newDF = df.take(10)
    assert(newDF.isInstanceOf[DataFrame] && newDF.size == 10)
  }

  test("drop") {
    val newDF = df.drop(10)
    assert(newDF.isInstanceOf[DataFrame] && newDF.size == 36634 - 10)
  }

  test("filter") {
    val newDF = df.filter(r => r.getAs[Int]("policyID").exists(_.toString.startsWith("1")))
    println(newDF.headOption)
    assert(newDF.isInstanceOf[DataFrame] && newDF.forall(_.getAs[Int]("policyID").exists(_.toString.startsWith("1"))))
  }
}
