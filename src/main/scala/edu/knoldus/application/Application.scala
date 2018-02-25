package edu.knoldus.application

import edu.knoldus.model.{FileOperator, LOG}

object Application extends App {
  try {
    val fileOperator = FileOperator()
    val resultRDD = fileOperator.getTotalSales
    fileOperator.stringifyResult(resultRDD)
    fileOperator.sparkContext.stop()
  } catch {
    case ex: Exception => LOG.info(s"\n ERROR: ${ex.getCause}");
  }

}
