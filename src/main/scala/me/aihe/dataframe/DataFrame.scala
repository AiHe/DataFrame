package me.aihe.dataframe

import me.aihe.dataframe.types._
import me.aihe.dataframe.util.{InferSchema, Parser}

import scala.collection.generic.CanBuildFrom
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Success

import scala.collection.{mutable, IndexedSeqLike}

/**
  * Created by aihe on 12/21/15.
  */

case class DataFrame(tableName: String, columns: Seq[GenericColumn] = Seq.empty) extends IndexedSeqLike[Row, BaseTable] with BaseTable {
  require(columns.map(_.name).distinct.length == columns.length)
  require(columns.isEmpty || columns.map(_.data.size).distinct.size == 1)

  val df: DataFrame = this

  val length = columns.length match {
    case 0 => 0
    case _ => columns.head.data.size
  }

  //  override val size = length

  val width = columns.length

  val columnNames = columns.map(_.name)

  val columnTypes = columns.map(_.dataType)

  lazy val rows = for (i <- 0 until length) yield Row(i, for (j <- 0 until width) yield columns(j)(i), columnNames)

  lazy val dataView = DataView(df, rows)

  lazy val columnsNameMap = Map(columnNames.zip(columns): _*)

  override def toString = {
    columnNames.mkString("", "\t", "\n") + dataView.map(_.data.mkString("\t")).mkString("\n")
  }

  def apply(index: Int): Row = {
    require(index >= 0 && index < length)
    dataView(index)
  }

  def apply(colName: String): GenericColumn = {
    require(columnNames.indexOf(colName) > 0)
    columnsNameMap(colName)
  }

  override def partition(p: Row => Boolean): (DataFrame, DataFrame) = {
    val (r1, r2) = dataView.partition(p(_))
    r1.toDataFrame -> r2.toDataFrame
  }

  override def take(n: Int): DataFrame = super.take(n).toDataFrame

  override def drop(n: Int): DataFrame = super.drop(n).toDataFrame

  override def takeWhile(p: (Row) => Boolean): DataFrame = super.takeWhile(p).toDataFrame

  override def dropRight(n: Int): DataFrame = super.dropRight(n).toDataFrame

  override def takeRight(n: Int): DataFrame = super.takeRight(n).toDataFrame

  override def filter(p: (Row) => Boolean): DataFrame = super.filter(p).toDataFrame

  override def filterNot(p: (Row) => Boolean): DataFrame = super.filterNot(p).toDataFrame

  override def dropWhile(p: (Row) => Boolean): DataFrame = super.dropWhile(p).toDataFrame

  override def splitAt(n: Int): (DataFrame, DataFrame) = {
    val (_1, _2) = super.splitAt(n)
    (_1.toDataFrame, _2.toDataFrame)
  }

  override def span(p: (Row) => Boolean): (DataFrame, DataFrame) = {
    val (_1, _2) = super.span(p)
    (_1.toDataFrame, _2.toDataFrame)
  }

  override def init: DataFrame = super.init.toDataFrame

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

  //  override def seq: IndexedSeq[Row] = dataView.seq

  override protected[this] def newBuilder: mutable.Builder[Row, DataView] = DataView.newBuilder(df)

  override val name: String = tableName

  override def toDataFrame: DataFrame = this

  override def toDataView: DataView = dataView

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

  private def feedColumn(columnName: String, columnType: DataType, columnData: Seq[Any]): GenericColumn = {
    if (columnType == IntType) {
      Column[Int](columnName, columnData.map(_.asInstanceOf[Int]), IntType)
    } else if (columnType == LongType) {
      Column[Long](columnName, columnData.map(_.asInstanceOf[Long]), LongType)
    } else if (columnType == DoubleType) {
      Column[Double](columnName, columnData.map(_.asInstanceOf[Double]), DoubleType)
    } else if (columnType == StringType) {
      Column[String](columnName, columnData.map(_.asInstanceOf[String]), StringType)
    } else {
      Column[Any](columnName, columnData, StringType)
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

  def fromRows(tableName: String, rows: Seq[Row] = Seq.empty, columnNames: Seq[String],
               columnTypes: Seq[DataType]): DataFrame = {
    val columns = columnNames.indices.map { case (i: Int) => Future(
      feedColumn(columnNames(i), columnTypes(i), rows.map(_ (i))))
    }

    val fs = Future.sequence(columns).map(DataFrame(tableName, _))

    Await.ready(fs, Duration.Inf).value.get
    match {
      case Success(t: DataFrame) => t
      case _ => throw new Exception
    }
  }
}
