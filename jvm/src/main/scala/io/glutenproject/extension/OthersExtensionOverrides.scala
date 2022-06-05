package io.glutenproject.extension

import io.glutenproject.{GlutenConfig, GlutenSparkExtensionsInjector}
import io.glutenproject.backendsapi.BackendsApiManager
import org.apache.spark.SparkEnv

import org.apache.spark.sql.SparkSessionExtensions

object OthersExtensionOverrides extends GlutenSparkExtensionsInjector {
  override def inject(extensions: SparkSessionExtensions): Unit = {
    // Spark extension rules for ClickHouse backend.
    if (SparkEnv.get.conf.get(GlutenConfig.GLUTEN_BACKEND_LIB)
      .equalsIgnoreCase(GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND)) {
      extensions.injectResolutionRule { session =>
        BackendsApiManager.getSparkPlanExecApiInstance
          .genExtendedAnalyzer(session, session.sessionState.conf)
      }
      extensions.injectPlannerStrategy { spark =>
        BackendsApiManager.getSparkPlanExecApiInstance.genExtendedDataSourceV2Strategy(spark)
      }
    }
  }
}
