package me.aihe

import scala.util.Try

/**
  * Created by aihe on 12/21/15.
  */
case class Row(table: DataFrame, index: Int) {
  require(index >= 0 && index < table.length)

  lazy val values = table.columns.map(_.data(index))

  val names = table.colNames

  lazy val valuesMap = Map(names.zip(values): _*)

  val length = names.length

  val size = length

  override def toString = {
    names.mkString("", "\t", "\n") + values.mkString("\t")
  }

  def apply(index: Int): Any = {
    values(index)
  }

  def getAs[T](i: Int): Try[T] = Try(apply(i).asInstanceOf[T])

  val columns = table.columns

}
