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
    assert(df.width == 18)
  }

  test("for expression") {
    val rows = for {
      r <- df
      pid <- r.getAs[Int]("policyID")
      if pid.toString.startsWith("1111")
    } yield r
    val newDF = DataFrame.fromRows("filtered", rows, df.columnNames, df.columnTypes)
    assert(newDF.forall(_.getAs[Int]("policyID").exists(_.toString.startsWith("1111"))))
  }
}
