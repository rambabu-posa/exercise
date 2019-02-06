package com.jpm.test

import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.mutable.ListBuffer
import collection.JavaConversions._

case class AppConfig(countryConfig: CountryConfig, schemaConf: List[List[String]],ignoredSymbols:List[String],defaultsConfig:DefaultsConfig)

case class CountryConfig(names: String, prefix: String, postfix: String)

case class DefaultsConfig(int: String, float: String, string: String)

object AppConfig {

  def apply(): AppConfig = {
    val conf: Config = ConfigFactory.load().resolve().getConfig("app")

    val countryConfig: CountryConfig = {
      val countryConf = conf.getConfig("countries")
      CountryConfig(
        countryConf.getString("names"),
        countryConf.getString("prefix"),
        countryConf.getString("postfix"))
    }



    val schemaConf: List[List[String]] = {
      val getConf = conf.getConfigList("schema")
      val states = new ListBuffer[List[String]]()
      for(i <- 0 until getConf.size) {
        states += List(
          getConf.get(i).getString("col_name"),
          getConf.get(i).getString("col_type")
        )
      }
      states.toList
    }

    val ignoredSymbols: List[String] = {
      val ignoredSymbolConf = conf.getConfig("ignored-symbols")
      ignoredSymbolConf.getStringList("symbols").toList
    }


    val defaultsConfig: DefaultsConfig = {
      val countryConf = conf.getConfig("defaults")
      DefaultsConfig(
        countryConf.getString("int"),
        countryConf.getString("float"),
        countryConf.getString("string"))
    }

    AppConfig(countryConfig, schemaConf,ignoredSymbols,defaultsConfig)
  }
}