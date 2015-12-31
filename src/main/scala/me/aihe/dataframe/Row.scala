package me.aihe.dataframe

import scala.util.Try

/**
  * Created by aihe on 12/21/15.
  */
case class Row(private[dataframe] val data: Seq[Any]) {
//  require(index >= 0 && index < table.length)

//  lazy val values = table.columns.map(_.data(index))

//  val names = table.columnNames

//  lazy val valuesMap = Map(names.zip(values): _*)

  val length = data.length

  val size = length

//  override def toString = {
//    names.mkString("", "\t", "\n") + values.mkString("\t")
//  }

  def apply(index: Int): Any = {
    data(index)
  }

  def get(index: Int): Any = data(index)

  def getAs[T](i: Int): Try[T] = Try(apply(i).asInstanceOf[T])

//  val columns = table.columns

//  def toSeq = values.map(_.toString)

}
