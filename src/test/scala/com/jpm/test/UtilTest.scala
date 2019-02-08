package com.jpm.test
import com.jpm.test.util.JpmTestUtils
import org.apache.spark.sql.types.IntegerType
import org.scalatest.{FlatSpec, Matchers}

class UtilTest  extends FlatSpec with Matchers with BaseSparkTest{

  "inferType " should " return the expected integer type" in {

    val result = JpmTestUtils.inferType("Int")
    val expected =  IntegerType
    result.equals(expected)
  }

  "getInt " should " return the expected integer value" in {

    val result = JpmTestUtils.getInt("10*",Map("*"->""))
    val expected =  10
    result.equals(expected)
  }

  "getDouble " should " return the expected double value" in {

    val result = JpmTestUtils.getInt("10.5*",Map("*"->""))
    val expected =  10.5
    result.equals(expected)
  }
}
