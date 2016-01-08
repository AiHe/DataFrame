package me.aihe.dataframe

import me.aihe.dataframe.types.IntType
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

  test("create") {
    val df1 = DataFrame("empty")
    assert(df1.length == 0)
    assert(df1.width == 0)
    val df2 = DataFrame("one", Seq(Column[Int]("c1", 0 until 5, IntType)))
    assert(df2.length == 5)
    assert(df2.width == 1)
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

  test("add column with wrong length") {
    intercept[IllegalArgumentException] {
      df.addColumn(Column[Int]("user_id", 1 to 36633, IntType))
    }
  }

  test("add column") {
    val newDF = df.addColumn(Column[Int]("user_id", 1 to 36634, IntType))
    assert(newDF.exists(_.getAs[Int]("user_id").exists(_ == 1)))
    assert(!newDF.exists(_.getAs[Int]("user_id").exists(_ == 0)))
  }

  //  test("add column 2") {
  //    val newDF = df.addColumn("user_id", 1 to 36634, IntType)
  //    assert(newDF.exists(_.getAs[Int]("user_id").exists(_ == 1)))
  //    assert(!newDF.exists(_.getAs[Int]("user_id").exists(_ == 0)))
  //    assert(df.width == 18)
  //    assert(newDF.width == 19)
  //  }

  test("remove column by index") {
    val newDF = df.removeColumn(0)
    assert(df.width == 18)
    assert(newDF.width == 17)
    assert(newDF.firstColumn.name != "policyID")
  }

  test("remove column by wrong index") {
    val newDF = df.removeColumn(-1)
    assert(df == newDF)
    assert(df eq newDF)
  }

  test("remove column by name") {
    val newDF = df.removeColumn("policyID")
    assert(df.width == 18)
    assert(newDF.width == 17)
  }

  test("remove column by wrong name") {
    val newDF = df.removeColumn("dummy")
    assert(df == newDF)
    assert(df eq newDF)
  }

  test("insert column") {
    val newDF = df.insertColumn(0, Column[Int]("user_id", 1 to 36634, IntType))
    assert(newDF.nameIndexMap("user_id") == 0)
    assert(newDF.exists(_.getAs[Int]("user_id").exists(_ == 1)))
    assert(!newDF.exists(_.getAs[Int]("user_id").exists(_ == 0)))
  }

  test("insert column with wrong length") {
    intercept[IllegalArgumentException] {
      df.insertColumn(0, Column[Int]("user_id", 1 to 36635, IntType))
    }
  }

  test("insert column at wrong position") {
    val newDF = df.insertColumn(-1, Column[Int]("user_id", 1 to 36634,
      IntType))
    assert(newDF.nameIndexMap("user_id") == 0)
    assert(newDF.exists(_.getAs[Int]("user_id").exists(_ == 1)))
    assert(!newDF.exists(_.getAs[Int]("user_id").exists(_ == 0)))
  }

  test("update column with wrong length") {
    intercept[IllegalArgumentException] {
      df.updateColumn(0, Column[Int]("user_id", 1 to 36635, IntType))
    }
  }

  test("update column by index") {
    val newDF = df.updateColumn(0, Column[Int]("user_id", 1 to 36634, IntType))
    assert(newDF.firstColumn.name == "user_id")
    assert(newDF.firstColumn.name != "policyID")
    assert(newDF.exists(_.getAs[Int]("user_id").exists(_ == 1)))
    assert(!newDF.exists(_.getAs[Int]("user_id").exists(_ == 0)))
  }

  test("update column by name") {
    val newDF = df.updateColumn("policyID", Column[Int]("user_id", 1 to 36634, IntType))
    assert(newDF.columnsNameMap.contains("user_id"))
    assert(!newDF.columnsNameMap.contains("policyID"))
    assert(newDF.exists(_.getAs[Int]("user_id").exists(_ == 1)))
    assert(!newDF.exists(_.getAs[Int]("user_id").exists(_ == 0)))
  }

  test("update column by wrong index") {
    val newDF = df.updateColumn(-1, Column[Int]("user_id", 1 to 36634, IntType))
    assert(df == newDF)
    assert(df eq newDF)
  }

  test("update column by wrong name") {
    val newDF = df.updateColumn("dummy", Column[Int]("user_id", 1 to 36634,
      IntType))
    assert(df == newDF)
    assert(df eq newDF)
  }

}
