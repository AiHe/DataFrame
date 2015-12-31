package me.aihe.dataframe.util

import org.apache.commons.csv.{CSVFormat, CSVParser}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

/**
  * Created by aihe on 12/21/15.
  */
object Parser {
  val defaultCsvFormat =
    CSVFormat.DEFAULT.withRecordSeparator(System.getProperty("line.separator", "\n"))

  def parseCSV(iter: Iterator[String]): IndexedSeq[Array[String]] = {
    iter.flatMap {
      case line =>
        Try(CSVParser.parse(line, defaultCsvFormat).getRecords.head.toArray) match {
          case Success(array) => Some(array)
          case Failure(_) => None
        }
    }.toIndexedSeq
  }

  def parseCSV(path: String): IndexedSeq[Array[String]] = {
    parseCSV(io.Source.fromFile(path, enc = "utf8").getLines())
  }

}
