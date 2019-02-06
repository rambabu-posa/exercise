package com.jpm.test.util

import com.jpm.test.Jpmo.{defaults, ignoreSymbols, schemaConf}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}

object jpm_test_utils {

  def inferType(field: String) = field match {
    case "Int" => IntegerType
    case "Double" => DoubleType
    case "String" => StringType
    case _ => StringType
  }

  def getInt(data : String,ignoreSymbolsMap:Map[String,String] ): Int ={
    val intData = ignoreSymbolsMap.foldLeft(data)((a, b) => a.replaceAllLiterally(b._1, b._2)).trim
    if(intData.trim matches """\d+""") intData.toInt else defaults.int.toInt
  }

  def getDouble(data : String,ignoreSymbolsMap:Map[String,String] ): Double ={
    val intData = ignoreSymbolsMap.foldLeft(data)((a, b) => a.replaceAllLiterally(b._1, b._2)).trim
    if(intData.trim matches """[+-]?([0-9]*[.])?[0-9]+""") intData.toDouble else defaults.float.toDouble
  }

  def validateData(line:String) : List[Any] = {
    val rowData = line.split("\\s+")
    var returnData = List[Any](rowData(0))
    var columnNumber = 1
    val ignoreSymbolsMap = Map(ignoreSymbols map((_,"")):_*)
    schemaConf.map(x=> {
      x(1) match {
        case "Int" => returnData = returnData:+getInt(rowData(columnNumber),ignoreSymbolsMap)
          columnNumber+=1

        case "Double" => returnData = returnData:+getDouble(rowData(columnNumber),ignoreSymbolsMap)
          columnNumber+=1

        case _ => returnData = returnData:+getInt(rowData(columnNumber),ignoreSymbolsMap)
          columnNumber+=1
      }
    })
    returnData
  }

  def filterInValidData(line:String) : Boolean = {
    if(line == "" ) return false
    line.split("\\s")(0) matches """\d+"""
  }


}
