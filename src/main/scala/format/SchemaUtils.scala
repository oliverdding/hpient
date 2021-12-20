package com.github.oliverdding.hpient
package format

import org.apache.spark.sql.types._

import scala.util.matching.Regex

object SchemaUtils {

  // format: off
  private[hpient] val arrayTypePattern:       Regex = """^Array\((.*)\)$""".r
  private[hpient] val dateTypePattern:        Regex = """^Date$""".r
  private[hpient] val dateTimeTypePattern:    Regex = """^DateTime(64)?(\((.*)\))?$""".r
  private[hpient] val decimalTypePattern:     Regex = """^Decimal\((\d+),\s*(\d+)\)$""".r
  private[hpient] val decimalTypePattern2:    Regex = """^Decimal(32|64|128|256)\((\d+)\)$""".r
  private[hpient] val enumTypePattern:        Regex = """^Enum(8|16)$""".r
  private[hpient] val fixedStringTypePattern: Regex = """^FixedString\((\d+)\)$""".r
  private[hpient] val nullableTypePattern:    Regex = """^Nullable\((.*)\)""".r
  // format: on

  def fromClickHouseType(chType: String): (DataType, Boolean) = {
    val (unwrappedChType, nullable) = unwrapNullable(chType)
    val catalystType = unwrappedChType match {
      case "String" | "LowCardinality(String)" | "UUID" | fixedStringTypePattern() | enumTypePattern(_) =>
        StringType
      case "Int8" => ByteType
      case "UInt8" | "Int16" => ShortType
      case "UInt16" | "Int32" => IntegerType
      case "UInt32" | "Int64" | "UInt64" | "IPv4" => LongType
      case "Int128" | "Int256" | "UInt256" =>
        throw new Exception(s"unsupported type: $chType") // not support
      case "Float32" => FloatType
      case "Float64" => DoubleType
      case dateTypePattern() => DateType
      case dateTimeTypePattern(_, _, _) => TimestampType
      case decimalTypePattern(precision, scale) =>
        DecimalType(precision.toInt, scale.toInt)
      case decimalTypePattern2(w, scale) =>
        w match {
          case "32" => DecimalType(9, scale.toInt)
          case "64" => DecimalType(18, scale.toInt)
          case "128" => DecimalType(38, scale.toInt)
          case "256" =>
            DecimalType(
              76,
              scale.toInt
            ) // throw exception, spark support precision up to 38
        }
      case arrayTypePattern(nestedChType) =>
        val (_chType, _nullable) = fromClickHouseType(nestedChType)
        ArrayType(_chType, _nullable)
      case _ => throw new Exception(s"Unsupported type: $chType")
    }
    (catalystType, nullable)
  }

  def toClickHouseType(catalystType: DataType): String =
    catalystType match {
      case BooleanType => "UInt8"
      case ByteType => "Int8"
      case ShortType => "Int16"
      case IntegerType => "Int32"
      case LongType => "Int64"
      case StringType => "String"
      case DateType => "Date"
      case TimestampType => "DateTime"
      case ArrayType(elemType, nullable) =>
        s"Array(${maybeNullable(toClickHouseType(elemType), nullable)})"
      case _ => throw new Exception(s"Unsupported type: $catalystType")
    }

  def fromClickHouseSchema(chSchema: Seq[(String, String)]): StructType = {
    val structFields = chSchema
      .map { case (name, maybeNullableType) =>
        val (catalyst, nullable) = fromClickHouseType(maybeNullableType)
        StructField(name, catalyst, nullable)
      }
    StructType(structFields)
  }

  def toClickHouseSchema(catalystSchema: StructType): Seq[(String, String)] =
    catalystSchema.fields
      .map { field =>
        val chType = toClickHouseType(field.dataType)
        (field.name, maybeNullable(chType, field.nullable))
      }

  private[hpient] def maybeNullable(
    chType: String,
    nullable: Boolean
  ): String =
    if (nullable) wrapNullable(chType) else chType

  private[hpient] def wrapNullable(chType: String): String =
    s"Nullable($chType)"

  private[hpient] def unwrapNullable(
    maybeNullableType: String
  ): (String, Boolean) = maybeNullableType match {
    case nullableTypePattern(typeName) => (typeName, true)
    case _ => (maybeNullableType, false)
  }
}
