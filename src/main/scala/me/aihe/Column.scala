package me.aihe

import me.aihe.types.DataType

/**
  * Created by aihe on 12/21/15.
  */
trait GenericColumn {
  val name: String
  val data: Seq[Any]
  val dataType: DataType
}

case class Column[T](name: String, data: Seq[T], override val dataType: DataType) extends GenericColumn {
  override def toString = {
    name + "\n" + data.mkString("\n")
  }
}
