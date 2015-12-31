package me.aihe.dataframe.util

import me.aihe.dataframe.types._

import scala.util.{Success, Try}

/**
  * Created by aihe on 12/21/15.
  */
object InferSchema {

  implicit def String2ExtendedString(s: String): StringConversion = new StringConversion(s)

  private val precedence: IndexedSeq[DataType] =
    IndexedSeq[DataType](
      IntType,
      LongType,
      DoubleType,
      StringType
    )

  private val findLowestCommonType: (DataType, DataType) => DataType = {
    case (t1, t2) if t1 == t2 => t1
    case (NullType, t1) => t1
    case (t1, NullType) => t1

    case (t1, t2) if Seq(t1, t2).forall(precedence.contains) =>
      val index = precedence.lastIndexWhere(t => t == t1 || t == t2)
      precedence(index)
  }

  private def inferType(t: DataType, s: String): DataType = {
    s.get(t)
  }

  private def mergeTypes(first: DataType, second: DataType): DataType = {
    findLowestCommonType(first, second)
  }

  def infer(columnData: Seq[String]): DataType = {
    columnData.aggregate[DataType](NullType)(inferType, mergeTypes)
  }
}

private[util] class StringConversion(s: String) {

  val f0 = () => Try(s.toInt) match {
    case Success(_) => Some(IntType)
    case _ => None
  }

  val f1 = () => Try(s.toLong) match {
    case Success(_) => Some(LongType)
    case _ => None
  }

  val f2 = () => Try(s.toDouble) match {
    case Success(_) => Some(DoubleType)
    case _ => None
  }


  def get(typeCur: DataType): DataType = {
    if (typeCur == NullType) {
      f0() orElse f1() orElse f2() getOrElse StringType
    } else if (typeCur == IntType) {
      f0() orElse f1() orElse f2() getOrElse StringType
    } else if (typeCur == LongType) {
      f1() orElse f2() getOrElse StringType
    } else if (typeCur == DoubleType) {
      f2() getOrElse StringType
    } else {
      StringType
    }
  }

}


