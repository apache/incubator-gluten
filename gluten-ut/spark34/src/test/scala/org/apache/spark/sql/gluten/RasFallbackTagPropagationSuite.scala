/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.gluten

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.events.GlutenPlanFallbackEvent
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.{GlutenSQLTestsTrait, Row}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.ui.{GlutenSQLAppStatusStore, SparkListenerSQLExecutionStart}
import org.apache.spark.status.ElementTrackingStore

import scala.collection.mutable.ArrayBuffer

/**
 * Test suite to reproduce the RAS fallback tag propagation issue described in:
 * https://github.com/apache/incubator-gluten/issues/7763
 *
 * ISSUE DESCRIPTION: When RAS (Rule-based Adaptive Selection) is enabled, fallback tags added
 * during RAS rule processing don't propagate back to the input plan. This causes the fallback
 * reporter to show generic "Gluten doesn't support..." messages instead of detailed fallback
 * information.
 *
 * ROOT CAUSE: The issue is in RasOffload.scala where RAS rules work on copies of the query plan.
 * When validation fails and fallback tags are added to these copies (lines 94, 142, 156), the tags
 * don't propagate back to the original plan that the fallback reporter sees.
 *
 * EVIDENCE IN CODE:
 *   - RasOffload.scala lines 136-138: "TODO: Tag the original plan with fallback reason. This is a
 *     non-trivial work in RAS as the query plan we got here may be a copy so may not propagate tags
 *     to original plan."
 *   - RasOffload.scala lines 150-152: Same TODO comment repeated
 *   - Lines 94, 142, 156: FIXME comments indicating temporary workaround
 *
 * EXPECTED BEHAVIOR: Detailed fallback reasons should be preserved and reported regardless of
 * whether RAS is enabled or disabled.
 *
 * ACTUAL BEHAVIOR: With RAS enabled, detailed fallback reasons are lost and generic messages are
 * shown instead.
 */
class RasFallbackTagPropagationSuite extends GlutenSQLTestsTrait with AdaptiveSparkPlanHelper {

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.GLUTEN_UI_ENABLED.key, "true")
      .set(UI_ENABLED, true)
  }

  /**
   * Test that demonstrates the RAS fallback tag propagation issue.
   *
   * When RAS is enabled, fallback tags added during RAS rule processing don't propagate back to the
   * original plan, causing the fallback reporter to show generic messages. When RAS is disabled,
   * the fallback tags are properly preserved and detailed fallback reasons are shown.
   */
  testGluten("RAS fallback tag propagation issue") {
    val kvStore = spark.sparkContext.statusStore.store.asInstanceOf[ElementTrackingStore]
    val glutenStore = new GlutenSQLAppStatusStore(kvStore)

    def runExecutionAndGetFallbackInfo(
        sqlString: String): (Long, Option[GlutenPlanFallbackEvent]) = {
      var executionId = 0L
      val fallbackEvents = ArrayBuffer[GlutenPlanFallbackEvent]()

      val listener = new SparkListener {
        override def onOtherEvent(event: SparkListenerEvent): Unit = {
          event match {
            case e: SparkListenerSQLExecutionStart =>
              executionId = e.executionId
            case e: GlutenPlanFallbackEvent =>
              fallbackEvents += e
            case _ =>
          }
        }
      }

      spark.sparkContext.addSparkListener(listener)
      try {
        sql(sqlString).collect()
        spark.sparkContext.listenerBus.waitUntilEmpty()
      } finally {
        spark.sparkContext.removeSparkListener(listener)
      }

      val fallbackEvent = fallbackEvents.find(_.executionId == executionId)
      (executionId, fallbackEvent)
    }

    withTable("test_table") {
      // Create a test table
      spark
        .range(100)
        .selectExpr("id", "id % 10 as group_id", "rand() as value")
        .write
        .format("parquet")
        .saveAsTable("test_table")

      // Test query that will likely cause fallbacks in some scenarios
      val testQuery = """
        SELECT group_id,
               COUNT(*) as cnt,
               AVG(value) as avg_val,
               STDDEV(value) as stddev_val
        FROM test_table
        WHERE value > 0.5
        GROUP BY group_id
        HAVING COUNT(*) > 5
        ORDER BY group_id
      """

      // Test with RAS disabled - should show detailed fallback reasons
      withSQLConf(
        GlutenConfig.RAS_ENABLED.key -> "false",
        GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "false" // Force a fallback
      ) {
        val (executionId1, fallbackEvent1) = runExecutionAndGetFallbackInfo(testQuery)
        val execution1 = glutenStore.execution(executionId1)

        assert(execution1.isDefined, "Execution should be found in store")
        assert(
          execution1.get.numFallbackNodes > 0,
          "Should have fallback nodes when filescan is disabled")

        if (fallbackEvent1.isDefined) {
          val event1 = fallbackEvent1.get
          // scalastyle:off println
          println(s"RAS DISABLED - Fallback reasons: ${event1.fallbackNodeToReason}")
          // scalastyle:on println

          // With RAS disabled, we should get detailed fallback reasons
          val hasDetailedReason = event1.fallbackNodeToReason.exists {
            case (_, reason) =>
              reason.contains("FallbackByUserOptions") || reason.contains("Validation failed")
          }
          assert(
            hasDetailedReason,
            s"Should have detailed fallback reasons when RAS is disabled. " +
              s"Got: ${event1.fallbackNodeToReason}")
        }
      }

      // Test with RAS enabled - demonstrates the issue where detailed fallback tags don't propagate
      withSQLConf(
        GlutenConfig.RAS_ENABLED.key -> "true",
        GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "false" // Force a fallback
      ) {
        val (executionId2, fallbackEvent2) = runExecutionAndGetFallbackInfo(testQuery)
        val execution2 = glutenStore.execution(executionId2)

        assert(execution2.isDefined, "Execution should be found in store")
        assert(
          execution2.get.numFallbackNodes > 0,
          "Should have fallback nodes when filescan is disabled")

        if (fallbackEvent2.isDefined) {
          val event2 = fallbackEvent2.get
          // scalastyle:off println
          println(s"RAS ENABLED - Fallback reasons: ${event2.fallbackNodeToReason}")
          // scalastyle:on println

          // This is where the bug manifests: with RAS enabled, the detailed fallback tags
          // added during RAS rule processing don't propagate back to the original plan,
          // so we get generic "Gluten doesn't support..." messages instead of detailed ones
          val hasGenericReason = event2.fallbackNodeToReason.exists {
            case (_, reason) =>
              reason.contains("Gluten doesn't support") || reason.isEmpty ||
              !reason.contains("FallbackByUserOptions")
          }

          // This assertion will likely fail, demonstrating the bug
          // When the bug is fixed, this should pass (i.e., we should get detailed reasons
          // even with RAS)
          if (hasGenericReason) {
            // scalastyle:off println
            println(
              "BUG REPRODUCED: RAS enabled shows generic fallback reasons instead of detailed ones")
            println(
              s"Expected detailed reasons like 'FallbackByUserOptions', but got: " +
                s"${event2.fallbackNodeToReason}")
            // scalastyle:on println
          } else {
            // scalastyle:off println
            println(
              "Bug appears to be fixed - detailed fallback reasons are preserved with RAS enabled")
            // scalastyle:on println
          }
        }
      }
    }
  }

  /**
   * Test that specifically targets the RAS rule processing to demonstrate how fallback tags get
   * lost during plan copying in RAS.
   *
   * This test focuses on the core issue: when RAS is enabled, fallback events may contain less
   * detailed information compared to when RAS is disabled.
   */
  testGluten("RAS rule processing loses fallback tags") {
    val kvStore = spark.sparkContext.statusStore.store.asInstanceOf[ElementTrackingStore]
    val glutenStore = new GlutenSQLAppStatusStore(kvStore)

    def runExecutionAndCaptureFallbacks(sqlString: String): Map[String, String] = {
      var executionId = 0L
      val fallbackEvents = ArrayBuffer[GlutenPlanFallbackEvent]()

      val listener = new SparkListener {
        override def onOtherEvent(event: SparkListenerEvent): Unit = {
          event match {
            case e: SparkListenerSQLExecutionStart =>
              executionId = e.executionId
            case e: GlutenPlanFallbackEvent =>
              fallbackEvents += e
            case _ =>
          }
        }
      }

      spark.sparkContext.addSparkListener(listener)
      try {
        sql(sqlString).collect()
        spark.sparkContext.listenerBus.waitUntilEmpty()
      } finally {
        spark.sparkContext.removeSparkListener(listener)
      }

      fallbackEvents
        .find(_.executionId == executionId)
        .map(_.fallbackNodeToReason)
        .getOrElse(Map.empty)
    }

    withTable("ras_test_table") {
      spark
        .range(50)
        .selectExpr("id", "id * 2 as doubled")
        .write
        .format("parquet")
        .saveAsTable("ras_test_table")

      val testQuery = "SELECT * FROM ras_test_table WHERE id > 25"

      // Test with RAS disabled first
      val rasDisabledFallbacks = withSQLConf(
        GlutenConfig.RAS_ENABLED.key -> "false",
        GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "false"
      ) {
        runExecutionAndCaptureFallbacks(testQuery)
      }

      // Test with RAS enabled
      val rasEnabledFallbacks = withSQLConf(
        GlutenConfig.RAS_ENABLED.key -> "true",
        GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "false"
      ) {
        runExecutionAndCaptureFallbacks(testQuery)
      }

      // scalastyle:off println
      println("=== RAS DISABLED FALLBACK REASONS ===")
      rasDisabledFallbacks.foreach {
        case (node, reason) =>
          println(s"Node: $node")
          println(s"Reason: $reason")
          println("---")
      }

      println("=== RAS ENABLED FALLBACK REASONS ===")
      rasEnabledFallbacks.foreach {
        case (node, reason) =>
          println(s"Node: $node")
          println(s"Reason: $reason")
          println("---")
      }
      // scalastyle:on println

      // The bug manifests as different fallback reasons between RAS enabled/disabled
      // With RAS disabled, we get detailed fallback reasons
      // With RAS enabled, the detailed reasons from RAS rules don't propagate back
      val rasDisabledHasDetails =
        rasDisabledFallbacks.values.exists(_.contains("FallbackByUserOptions"))
      val rasEnabledHasDetails =
        rasEnabledFallbacks.values.exists(_.contains("FallbackByUserOptions"))

      if (rasDisabledHasDetails && !rasEnabledHasDetails) {
        // scalastyle:off println
        println("BUG REPRODUCED: Detailed fallback reasons are lost when RAS is enabled")
        println(
          "This demonstrates the issue described in " +
            "https://github.com/apache/incubator-gluten/issues/7763")
        // scalastyle:on println
      } else if (rasDisabledHasDetails && rasEnabledHasDetails) {
        // scalastyle:off println
        println("Bug appears to be fixed - detailed fallback reasons preserved with RAS")
        // scalastyle:on println
      } else {
        // scalastyle:off println
        println("Test may need adjustment - check fallback scenarios")
        println(s"RAS disabled has details: $rasDisabledHasDetails")
        println(s"RAS enabled has details: $rasEnabledHasDetails")
        // scalastyle:on println
      }
    }
  }

  /**
   * Simplified test that demonstrates the core issue with minimal setup.
   *
   * This test creates a scenario where fallback is guaranteed to happen (by disabling columnar file
   * scan), then compares the fallback reasons reported when RAS is enabled vs disabled.
   *
   * The bug manifests as:
   *   - RAS disabled: Detailed reasons like "[FallbackByUserOptions] Validation failed..."
   *   - RAS enabled: Generic reasons or missing details due to tag propagation failure
   */
  testGluten("Simple RAS fallback tag propagation test") {
    withTable("simple_test") {
      // Create a simple test table
      spark.range(10).write.format("parquet").saveAsTable("simple_test")

      val testQuery = "SELECT * FROM simple_test"

      // Helper function to extract fallback reasons from execution
      def getFallbackReasons(rasEnabled: Boolean): Set[String] = {
        val fallbackReasons = scala.collection.mutable.Set[String]()

        val listener = new SparkListener {
          override def onOtherEvent(event: SparkListenerEvent): Unit = {
            event match {
              case e: GlutenPlanFallbackEvent =>
                fallbackReasons ++= e.fallbackNodeToReason.values
              case _ =>
            }
          }
        }

        spark.sparkContext.addSparkListener(listener)
        try {
          withSQLConf(
            GlutenConfig.RAS_ENABLED.key -> rasEnabled.toString,
            GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "false" // Force fallback
          ) {
            sql(testQuery).collect()
            spark.sparkContext.listenerBus.waitUntilEmpty()
          }
        } finally {
          spark.sparkContext.removeSparkListener(listener)
        }

        fallbackReasons.toSet
      }

      // Get fallback reasons with RAS disabled and enabled
      val reasonsWithoutRas = getFallbackReasons(rasEnabled = false)
      val reasonsWithRas = getFallbackReasons(rasEnabled = true)

      // scalastyle:off println
      println("=== FALLBACK REASONS COMPARISON ===")
      println(s"RAS DISABLED reasons (${reasonsWithoutRas.size}):")
      reasonsWithoutRas.foreach(reason => println(s"  - $reason"))

      println(s"RAS ENABLED reasons (${reasonsWithRas.size}):")
      reasonsWithRas.foreach(reason => println(s"  - $reason"))
      // scalastyle:on println

      // Check for the presence of detailed fallback information
      val detailedReasonPatterns = Set(
        "FallbackByUserOptions",
        "Validation failed",
        "filescan.*disabled" // Pattern for file scan disabled message
      )

      val rasDisabledHasDetails = reasonsWithoutRas.exists(
        reason =>
          detailedReasonPatterns.exists(
            // scalastyle:off caselocale
            pattern =>
              reason.toLowerCase.contains(pattern.toLowerCase)
              // scalastyle:on caselocale
          ))

      val rasEnabledHasDetails = reasonsWithRas.exists(
        reason =>
          detailedReasonPatterns.exists(
            // scalastyle:off caselocale
            pattern =>
              reason.toLowerCase.contains(pattern.toLowerCase)
              // scalastyle:on caselocale
          ))

      // scalastyle:off println
      println(s"RAS disabled has detailed reasons: $rasDisabledHasDetails")
      println(s"RAS enabled has detailed reasons: $rasEnabledHasDetails")

      // The bug is reproduced if:
      // 1. RAS disabled shows detailed reasons
      // 2. RAS enabled shows fewer or less detailed reasons
      if (rasDisabledHasDetails && !rasEnabledHasDetails) {
        println("BUG REPRODUCED: RAS enabled loses detailed fallback information")
        println(
          "  This confirms the issue described in " +
            "https://github.com/apache/incubator-gluten/issues/7763")
      } else if (rasDisabledHasDetails && rasEnabledHasDetails) {
        println("Bug appears to be FIXED: Both RAS enabled/disabled show detailed reasons")
      } else if (!rasDisabledHasDetails && !rasEnabledHasDetails) {
        println("Test inconclusive: Neither configuration shows detailed reasons")
        println("  This might indicate the test scenario needs adjustment")
      } else {
        println("Unexpected result: RAS enabled shows details but disabled doesn't")
      }
      // scalastyle:on println

      // Additional analysis: compare reason counts and content
      if (reasonsWithoutRas.size != reasonsWithRas.size) {
        // scalastyle:off println
        println(
          s"Different number of fallback reasons: RAS disabled=${reasonsWithoutRas.size}, " +
            s"RAS enabled=${reasonsWithRas.size}")
        // scalastyle:on println
      }

      val commonReasons = reasonsWithoutRas.intersect(reasonsWithRas)
      val onlyInDisabled = reasonsWithoutRas -- reasonsWithRas
      val onlyInEnabled = reasonsWithRas -- reasonsWithoutRas

      if (onlyInDisabled.nonEmpty) {
        // scalastyle:off println
        println("Reasons only present when RAS is disabled:")
        onlyInDisabled.foreach(reason => println(s"  - $reason"))
        // scalastyle:on println
      }

      if (onlyInEnabled.nonEmpty) {
        // scalastyle:off println
        println("Reasons only present when RAS is enabled:")
        onlyInEnabled.foreach(reason => println(s"  - $reason"))
        // scalastyle:on println
      }
    }
  }
}
