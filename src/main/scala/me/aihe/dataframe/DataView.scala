package me.aihe.dataframe

import scala.collection.{mutable, IndexedSeqLike}

/**
  * Created by aihe on 1/4/16.
  */
trait BaseTable extends IndexedSeq[Row] {
  def name: String

  def columns: Seq[GenericColumn]

  def rows: IndexedSeq[Row]

  def length: Int

  def toDataView: DataView

  def toDataFrame: DataFrame
}


private[dataframe] class DataView(private[dataframe] val dataFrame: DataFrame, override val rows: IndexedSeq[Row])
  extends IndexedSeqLike[Row, DataView] with BaseTable {
  override def seq: IndexedSeq[Row] = rows

  override def length: Int = rows.length

  override def apply(idx: Int): Row = rows(idx)

  override protected[this] def newBuilder: mutable.Builder[Row, DataView] = DataView.newBuilder(dataFrame)

  override val name: String = dataFrame.name

  override def toDataFrame: DataFrame = DataView.toDataTable(this)

  override def toDataView: DataView = this

  override val columns: Seq[GenericColumn] = dataFrame.columns
}

object DataView {

  /** Builder for a new DataView. */
  def newBuilder(dataFrame: DataFrame): mutable.Builder[Row, DataView] =
    Vector.newBuilder[Row] mapResult (vector => DataView(dataFrame, vector))

  def apply(sourceDataFrame: DataFrame, dataRows: Iterable[Row]): DataView = {
    new DataView(sourceDataFrame, dataRows.toIndexedSeq)
  }

  /** Builds a new DataTable from a DataView. */
  def toDataTable(dataView: DataView): DataFrame = {
    val rowIndexes = dataView.map(row => row.index)
    val newColumns = dataView.columns.map(col => col.fromRows(rowIndexes))
    DataFrame(dataView.name, newColumns)
  }
}