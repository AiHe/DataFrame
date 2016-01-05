package me.aihe.dataframe

import scala.util.Try

import me.aihe.dataframe.types._

/**
  * Created by aihe on 12/21/15.
  */
trait GenericColumn {
  val name: String
  val data: Seq[Any]
  val dataType: DataType

  def apply(index: Int): Any = {
    data(index)
  }

  def get(index: Int): Any = data(index)

  def getAs[T](i: Int): Try[T] = Try(apply(i).asInstanceOf[T])

  def fromRows(rowIndexes: IndexedSeq[Int]): GenericColumn = {
    Column(name, rowIndexes.map(data(_)), dataType)
  }
}

case class Column[T](name: String, data: Seq[T], override val dataType: DataType) extends GenericColumn {
  override def toString = {
    name + "\n" + data.mkString("\n")
  }
}
