package me.aihe.dataframe

import scala.util.Try

/**
  * Created by aihe on 12/21/15.
  */
case class Row(private[dataframe] val index: Int, private[dataframe] val data: Seq[Any], private[dataframe] val names: Seq[String]) {
  //  require(index >= 0 && index < table.length)

  //  lazy val values = table.columns.map(_.data(index))

  //  val names = table.columnNames

  lazy val valuesMap = Map(names.zip(data): _*)

  val length = data.length

  val size = length

  //  override def toString = {
  //    names.mkString("", "\t", "\n") + values.mkString("\t")
  //  }

  def apply(index: Int): Any = {
    require(0 <= index && index < data.size)
    data(index)
  }

  def apply(name: String): Any = {
    require(valuesMap.contains(name))
    valuesMap(name)
  }

  def get(index: Int): Any = apply(index)

  def get(name: String): Any = apply(name)

  def getAs[T](i: Int): Option[T] = Try(apply(i).asInstanceOf[T]).toOption

  def getAs[T](name: String): Option[T] = Try(apply(name).asInstanceOf[T]).toOption

  //  val columns = table.columns

  //  def toSeq = values.map(_.toString)

}
