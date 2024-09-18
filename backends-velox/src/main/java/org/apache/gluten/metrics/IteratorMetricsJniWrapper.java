package org.apache.gluten.metrics;

import org.apache.gluten.runtime.Runtime;
import org.apache.gluten.runtime.RuntimeAware;
import org.apache.gluten.runtime.Runtimes;
import org.apache.gluten.vectorized.ColumnarBatchOutIterator;

public class IteratorMetricsJniWrapper implements RuntimeAware {
  private final Runtime runtime;

  private IteratorMetricsJniWrapper(Runtime runtime) {
    this.runtime = runtime;
  }

  public static IteratorMetricsJniWrapper create() {
    final Runtime runtime = Runtimes.contextInstance("IteratorMetrics");
    return new IteratorMetricsJniWrapper(runtime);
  }

  public Metrics fetch(ColumnarBatchOutIterator out) {
    return nativeFetchMetrics(out.handle());
  }

  private native Metrics nativeFetchMetrics(long itrHandle);

  @Override
  public long handle() {
    return runtime.getHandle();
  }
}
