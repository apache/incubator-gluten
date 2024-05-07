package org.apache.gluten.extension.columnar.enumerated

import org.apache.gluten.extension.columnar.rewrite.RewriteSingleNode
import org.apache.gluten.ras.rule.{RasRule, Shape}

import org.apache.spark.sql.execution.SparkPlan

object PreRewrittenRule {
  trait Rewrite {
    def rewrite(node: SparkPlan): SparkPlan
  }

  object Rewrite {
    implicit class FromRewriteSingleNodeRules(r: Seq[RewriteSingleNode]) extends Rewrite {
      override def rewrite(node: SparkPlan): SparkPlan = {
        val out = r.foldLeft(node) {
          case (node, rewrite) =>
            rewrite.rewrite(node)
        }
        out
      }
    }
  }

  def wrap(rule: RasRule[SparkPlan], rewrite: Rewrite): RasRule[SparkPlan] = {
    new RasRule[SparkPlan] {
      override def shift(node: SparkPlan): Iterable[SparkPlan] = {
        val out = List(node)
          .map(rewrite.rewrite)
          .flatMap(rule.shift)
        out
      }

      override def shape(): Shape[SparkPlan] = rule.shape()
    }
  }
}
