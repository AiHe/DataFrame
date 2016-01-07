package me.aihe.dataframe

import me.aihe.dataframe.types._
import me.aihe.dataframe.util.{InferSchema, Parser}

import scala.collection.IndexedSeq._
import scala.collection.Seq._
import scala.collection._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Success

/**
  * Created by aihe on 12/21/15.
  */

case class DataFrame(tableName: String, columns: Seq[GenericColumn] = Seq.empty)
  extends IndexedSeqOptimized[Row, DataFrame]
  with IndexedSeq[Row] {

  require(columns.map(_.name).distinct.length == columns.length)
  require(columns.isEmpty || columns.map(_.data.size).distinct.size == 1)

  val df: DataFrame = this

  val length = columns.length match {
    case 0 => 0
    case _ => columns.head.data.size
  }

  val width = columns.length

  val columnNames = columns.map(_.name)

  val columnTypes = columns.map(_.dataType)

  lazy val rows = for (i <- 0 until length) yield Row(i, for (j <- 0 until width) yield columns(j)(i), columnNames)

  lazy val columnsNameMap = Map(columnNames.zip(columns): _*)

  lazy val nameIndexMap = Map(columnNames.zipWithIndex: _*)

  lazy val indexNameMap = Map((0 until width).zip(columnNames): _*)


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

  def column(col: Int) = {
    require(col >= 0 && col < width)
    columns(col)
  }

  def firstColumn = columns.head

  def lastColumn = columns.last

  def lastColumnOption = columns.lastOption


  override protected[this] def newBuilder: mutable.Builder[Row, DataFrame] = {
    DataFrame.newBuilder(df)
  }

  /*override def map[B](f: (Row) => B): DataFrame = {
    val b = newBuilder
    for (x <- this) b += f(x) // take Row
    b.result // build DataFrame
    The newBuider cannot be used in map because newBuilder will take Row to build DataFrame,
    but the result type of function f is B.
  }*/

  val name: String = tableName

  def addColumn[T](column: Column[T]): DataFrame = {
    this.copy(columns = columns :+ column)
  }

  def addColumn[T](name: String, data: Seq[T], dataType: DataType): DataFrame = {
    addColumn(Column(name, data, dataType))
  }

  def insertColumn[T](idx: Int, column: Column[T]): DataFrame = {
    val (pre, suf) = columns.splitAt(idx)
    this.copy(columns = (pre :+ column) ++ suf)
  }

  def insertColumn[T](idx: Int, name: String, data: Seq[T], dataType: DataType): DataFrame = {
    insertColumn(idx, Column(name, data, dataType))
  }

  def removeColumn(idx: Int): DataFrame = {
    val (pre, suf) = columns.splitAt(idx)
    this.copy(columns = pre ++ suf.drop(1))
  }

  def removeColumn(name: String): DataFrame = {
    removeColumn(nameIndexMap(name))
  }

  def updateColumn[T](idx: Int, column: Column[T]): DataFrame = {
    this.copy(columns = columns.updated(idx, column))
  }

  def updateColumn[T](name: String, column: Column[T]): DataFrame = {
    updateColumn(nameIndexMap(name), column)
  }
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

    val columns = h.indices.map { case (i: Int) => Future(feedColumn(h(i), d.map(_ (i)))) }

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

  private def newBuilder(dataFrame: DataFrame): mutable.Builder[Row, DataFrame] = {
    Vector.newBuilder[Row] mapResult (vector => {
      val rowIndexes = vector.map(_.index)
      val newColumns = dataFrame.columns.map(col => col.fromRows(rowIndexes))
      DataFrame(dataFrame.name, newColumns)
    })
  }

  def fromRows(dataFrame: DataFrame, rows: IndexedSeq[Row]): DataFrame = {
    val rowIndexes = rows.map(_.index)
    val newColumns = dataFrame.columns.map(col => col.fromRows(rowIndexes))
    DataFrame(dataFrame.name, newColumns)
  }
}
