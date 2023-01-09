package org.apache.spark.sql

import org.apache.commons.math3.util.Precision

import org.apache.spark.sql.catalyst.util.sideBySide

object GlutenTestUtils {
  private val DOUBLE_TOLERANCE = 0.00001D // 0.001%

  class FuzzyDouble(private val value: Double) extends Comparable[FuzzyDouble] {
    override def equals(anotherDouble: Any): Boolean = anotherDouble match {
      case d: FuzzyDouble =>
        Precision.equalsWithRelativeTolerance(value, d.value, DOUBLE_TOLERANCE)
      case _ => false
    }
    override def toString: String = java.lang.Double.toString(value)

    // unsupported
    override def compareTo(anotherDouble: FuzzyDouble): Int = throw new UnsupportedOperationException
    override def hashCode(): Int = throw new UnsupportedOperationException
  }

  class FuzzyFloat(private val value: Float) extends Comparable[FuzzyFloat] {
    override def equals(anotherFloat: Any): Boolean = anotherFloat match {
      case d: FuzzyFloat =>
        Precision.equalsWithRelativeTolerance(value, d.value, DOUBLE_TOLERANCE)
      case _ => false
    }
    override def toString: String = java.lang.Float.toString(value)

    // unsupported
    override def compareTo(anotherFloat: FuzzyFloat): Int = throw new UnsupportedOperationException
    override def hashCode(): Int = throw new UnsupportedOperationException
  }

  // Derived from org.apache.spark.sql.test.SQLTestUtils.compareAnswers
  def compareAnswers(
    sparkAnswer: Seq[Row],
    expectedAnswer: Seq[Row],
    sort: Boolean): Option[String] = {
    def prepareAnswer(answer: Seq[Row]): Seq[Row] = {
      // Converts data to types that we can do equality comparison using Scala collections.
      // For BigDecimal type, the Scala type has a better definition of equality test (similar to
      // Java's java.math.BigDecimal.compareTo).
      // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
      // equality test.
      // This function is copied from Catalyst's QueryTest
      val converted: Seq[Row] = answer.map { s =>
        Row.fromSeq(s.toSeq.map {
          case d: java.math.BigDecimal => BigDecimal(d)
          case b: Array[Byte] => b.toSeq
          case f: Float => new FuzzyFloat(f)
          case db: Double => new FuzzyDouble(db)
          case o => o
        })
      }
      if (sort) {
        converted.sortBy(_.toString())
      } else {
        converted
      }
    }
    if (prepareAnswer(expectedAnswer) != prepareAnswer(sparkAnswer)) {
      val errorMessage =
        s"""
           | == Results ==
           | ${sideBySide(
          s"== Expected Answer - ${expectedAnswer.size} ==" +:
            prepareAnswer(expectedAnswer).map(_.toString()),
          s"== Actual Answer - ${sparkAnswer.size} ==" +:
            prepareAnswer(sparkAnswer).map(_.toString())).mkString("\n")}
      """.stripMargin
      Some(errorMessage)
    } else {
      None
    }
  }
}
