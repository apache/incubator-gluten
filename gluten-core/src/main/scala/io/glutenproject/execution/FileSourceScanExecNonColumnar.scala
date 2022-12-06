package io.glutenproject.execution

import org.apache.spark.sql.execution.FileSourceScanExec

class FileSourceScanExecNonColumnar extends FileSourceScanExec {

  override supportsColumnar: Boolean = {
    false
  }
}
