package io.glutenproject.integration.tpc;

import io.glutenproject.integration.tpc.ds.TpcdsSuite;
import io.glutenproject.integration.tpc.h.TpchSuite;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import picocli.CommandLine;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Callable;


@CommandLine.Command(name = "gluten-tpc", mixinStandardHelpOptions = true,
    showDefaultValues = true,
    description = "Gluten integration test using TPC benchmark's data and queries")
public class Tpc implements Callable<Integer> {

  @CommandLine.Option(required = true, names = {"--benchmark-type"}, description = "TPC benchmark type: h, ds")
  private String benchmarkType;

  @CommandLine.Option(required = true, names = {"-b", "--backend-type"}, description = "Backend used: vanilla, velox, gazelle-cpp, ...")
  private String backendType;

  @CommandLine.Option(names = {"--baseline-backend-type"}, description = "Baseline backend used: vanilla, velox, gazelle-cpp, ...", defaultValue = "vanilla")
  private String baselineBackendType;

  @CommandLine.Option(names = {"-s", "--scale"}, description = "The scale factor of sample TPC-H dataset", defaultValue = "0.1")
  private double scale;

  @CommandLine.Option(names = {"--fixed-width-as-double"}, description = "Generate integer/long/date as double", defaultValue = "false")
  private boolean fixedWidthAsDouble;

  @CommandLine.Option(names = {"--queries"}, description = "Set a comma-seperated list of query IDs to run, run all queries if not specified. Example: --queries=q1,q6", split = ",", defaultValue = "__all__")
  private String[] queries;

  @CommandLine.Option(names = {"--log-level"}, description = "Set log level: 0 for DEBUG, 1 for INFO, 2 for WARN", defaultValue = "2")
  private int logLevel;

  @CommandLine.Option(names = {"--explain"}, description = "Output explain result for queries", defaultValue = "false")
  private boolean explain;

  @CommandLine.Option(names = {"--error-on-memleak"}, description = "Fail the test when memory leak is detected by Spark's memory manager", defaultValue = "false")
  private boolean errorOnMemLeak;

  @CommandLine.Option(names = {"--enable-history"}, description = "Start a Spark history server during running", defaultValue = "false")
  private boolean enableHsUi;

  @CommandLine.Option(names = {"--history-ui-port"}, description = "Port that Spark history server UI binds to", defaultValue = "18080")
  private int hsUiPort;

  @CommandLine.Option(names = {"--cpus"}, description = "Executor cpu number", defaultValue = "2")
  private int cpus;

  @CommandLine.Option(names = {"--off-heap-size"}, description = "Off heap memory size per executor", defaultValue = "6g")
  private String offHeapSize;

  @CommandLine.Option(names = {"--iterations"}, description = "How many iterations to run", defaultValue = "1")
  private int iterations;

  @CommandLine.Option(names = {"--disable-aqe"}, description = "Disable Spark SQL adaptive query execution", defaultValue = "false")
  private boolean disableAqe;

  @CommandLine.Option(names = {"--disable-bhj"}, description = "Disable Spark SQL broadcast hash join", defaultValue = "false")
  private boolean disableBhj;

  @CommandLine.Option(names = {"--disable-wscg"}, description = "Disable Spark SQL whole stage code generation", defaultValue = "false")
  private boolean disableWscg;

  @CommandLine.Option(names = {"--partition"}, description = "Data has partition", defaultValue = "false")
  private boolean partition;

  @CommandLine.Option(names = {"--file-format"}, description = "Option: parquet, dwrf", defaultValue = "parquet")
  private String fileFormat;

  @CommandLine.Option(names = {"--conf"}, description = "Test line Spark conf, --conf=k1=v1 --conf=k2=v2")
  private Map<String, String> sparkConf;

  @CommandLine.Option(names = {"--use-existing-data"}, description = "Use the generated data in /tmp/tpcds-generated or other, when it is true, then value of --scale will be ignored", defaultValue = "false")
  private boolean useExistingData;

  public Tpc() {
  }

  private SparkConf pickSparkConf(String backendType) {
    SparkConf conf;
    switch (backendType) {
      case "vanilla":
        conf = Constants.VANILLA_CONF();
        break;
      case "velox":
        conf = Constants.VELOX_BACKEND_CONF();
        break;
      case "gazelle-cpp":
        conf = Constants.GAZELLE_CPP_BACKEND_CONF();
        break;
      default:
        throw new IllegalArgumentException("Backend type not found: " + backendType);
    }
    return conf;
  }

  @Override
  public Integer call() throws Exception {
    final SparkConf baselineConf = pickSparkConf(baselineBackendType);
    final SparkConf testConf = pickSparkConf(backendType);
    if (sparkConf != null) {
      sparkConf.forEach(testConf::set);
      sparkConf.forEach(baselineConf::set);
    }
    final Level level;
    switch (logLevel) {
      case 0:
        level = Level.DEBUG;
        break;
      case 1:
        level = Level.INFO;
        break;
      case 2:
        level = Level.WARN;
        break;
      default:
        throw new IllegalArgumentException("Log level not found: " + logLevel);
    }
    final TpcSuite suite;
    switch (benchmarkType) {
      case "h":
        suite = new TpchSuite(testConf, baselineConf, scale,
                fixedWidthAsDouble, queries, level, explain, errorOnMemLeak,
                enableHsUi, hsUiPort, cpus, offHeapSize, iterations, disableAqe, disableBhj,
            disableWscg, useExistingData);
        break;
      case "ds":
        suite = new TpcdsSuite(testConf, baselineConf, scale,
            fixedWidthAsDouble, queries, level, explain, errorOnMemLeak,
            enableHsUi, hsUiPort, cpus, offHeapSize, iterations, disableAqe, disableBhj,
            disableWscg, partition, fileFormat, useExistingData);
        break;
      default:
        throw new IllegalArgumentException("TPC benchmark type not found: " + benchmarkType);
    }
    boolean succeed = suite.run();
    if (!succeed) {
      return -1;
    }
    return 0;
  }

  public static void main(String... args) {
    int exitCode = new CommandLine(new Tpc()).execute(args);
    System.exit(exitCode);
  }
}
