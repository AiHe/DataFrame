package me.aihe.dataframe

import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by aihe on 12/21/15.
  */
class DataFrameSuite extends FunSuite with BeforeAndAfterAll {

  test("load csv") {
    val df = DataFrame.loadCSV(path = "data/sample.csv", tableName = "fraud", header = true)
    assert(df.length == 36634)
    assert(df.width == 18)
  }

}
