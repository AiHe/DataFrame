package me.aihe.dataframe

import me.aihe.dataframe.types.{DoubleType, IntType, LongType, StringType}
import me.aihe.dataframe.util.{InferSchema, Parser}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Success

/**
  * Created by aihe on 12/21/15.
  */

case class DataFrame(tableName: String, columns: Seq[GenericColumn] = Seq.empty) extends Traversable[Row] {
  require(columns.map(_.name).distinct.length == columns.length)
  require(columns.isEmpty || columns.map(_.data.size).distinct.size == 1)

  val table: DataFrame = this

  val length = columns.length match {
    case 0 => 0
    case _ => columns.head.data.size
  }

  override val size = length

  val width = columns.length

  val columnNames = columns.map(_.name)

  //  lazy val rows = (0 until length).map(i => Row((0 until width).map(j => columns(j)(i))))

  lazy val rows = for (i <- 0 until length) yield Row(for (j <- 0 until width) yield columns(j)(i))

  lazy val columnsNameMap = Map(columnNames.zip(columns): _*)

  override def toString = {
    columnNames.mkString("", "\t", "\n") + rows.map(_.data.mkString("\t")).mkString("\n")
  }

  def apply(index: Int): Row = {
    require(index >= 0 && index < length)
    rows(index)
  }

  def apply(colName: String): GenericColumn = {
    require(columnNames.indexOf(colName) > 0)
    columnsNameMap(colName)
  }

  override def nonEmpty: Boolean = length > 0

  override def isEmpty: Boolean = !nonEmpty

  override def head: Row = rows.head

  override def last: Row = rows.last

  override def headOption: Option[Row] = rows.headOption

  //  def partition(p: Row => Boolean): (Table, Table) = {
  //    val (r1, r2) = rows.partition(p(_))
  //    Table.fromRows(tableName + "_true", r1) -> Table.fromRows(tableName + "_false", r2)
  //  }

  def column(col: Int) = {
    require(col >= 0 && col < width)
    columns(col)
  }

  def firstColumn = {
    columns.head
  }

  def lastColumn = {
    columns.last
  }

  def lastColumnOption = {
    columns.lastOption
  }

  def map[T](f: Row => T): Traversable[T] = {
    rows.map(f)
  }

  //  def map(f: Row => Array[String]): DataFrame = {
  //    load(rows.map(f), tableName)
  //  }

  override def foreach[U](f: (Row) => U): Unit = ???
}

case object DataFrame {

  private def feedColumn(columnName: String, columnData: Seq[String]): GenericColumn = {
    val dType = InferSchema.infer(columnData)
    if (dType == IntType) {
      Column[Int](columnName, columnData.map(_.toInt), IntType)
    } else if (dType == LongType) {
      Column[Long](columnName, columnData.map(_.toLong), LongType)
    } else if (dType == DoubleType) {
      Column[Double](columnName, columnData.map(_.toDouble), DoubleType)
    } else {
      Column[String](columnName, columnData, StringType)
    }
  }

  def load(data: Seq[Array[String]], tableName: String, header: Boolean = false): DataFrame = {
    val (h, d) = if (header) {
      val (a, b) = data.splitAt(1)
      (a.head, b)
    } else {
      (data.head.map("col" + _), data)
    }

    val columns = h.indices.map { case (i: Int) => Future(feedColumn(h(i), d.map(_ (i)).toIndexedSeq)) }

    val fs = Future.sequence(columns).map(DataFrame(tableName, _))

    Await.ready(fs, Duration.Inf).value.get
    match {
      case Success(t: DataFrame) => t
      case _ => throw new Exception
    }
  }

  def loadCSV(path: String, tableName: String, header: Boolean): DataFrame = {
    load(Parser.parseCSV(path), tableName, header)
  }
}
