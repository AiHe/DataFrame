package me.aihe

import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by aihe on 12/21/15.
  */
class DataFrameSuite extends FunSuite with BeforeAndAfterAll {

  test("load csv") {
    DataFrame.loadCSV(path = "/Users/aihe/Documents/workspace/fraud-detection/oct_cr_order2.txt",
      tableName = "fraud", header = true)
  }

}
