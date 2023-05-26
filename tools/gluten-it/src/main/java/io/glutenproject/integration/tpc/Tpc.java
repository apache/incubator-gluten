package io.glutenproject.integration.tpc;

import io.glutenproject.integration.tpc.ds.TpcdsSuite;
import io.glutenproject.integration.tpc.h.TpchSuite;
import io.glutenproject.integration.tpc.action.Action;
import io.glutenproject.integration.tpc.action.Actions;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import picocli.CommandLine;
import scala.Predef;
import scala.collection.JavaConverters;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;


@CommandLine.Command(name = "gluten-tpc", mixinStandardHelpOptions = true,
    showDefaultValues = true,
    description = "Gluten integration test using TPC benchmark's data and queries")
public class Tpc implements Callable<Integer> {

  @CommandLine.Option(required = true, names = {"--benchmark-type"}, description = "TPC benchmark type: h, ds", defaultValue = "h")
  private String benchmarkType;

  @CommandLine.Option(names = {"--skip-data-gen"}, description = "Skip data generation", defaultValue = "false")
  private boolean skipDataGen;

  @CommandLine.Option(names = {"--mode"}, description = "Mode: data-gen-only, queries, queries-compare, spark-shell", defaultValue = "queries-compare")
  private String mode;

  @CommandLine.Option(names = {"-p", "--preset"}, description = "Preset used: vanilla, velox, velox-with-celeborn...", defaultValue = "velox")
  private String preset;

  @CommandLine.Option(names = {"--baseline-preset"}, description = "Baseline preset used: vanilla, velox, velox-with-celeborn...", defaultValue = "vanilla")
  private String baselinePreset;

  @CommandLine.Option(names = {"-s", "--scale"}, description = "The scale factor of sample TPC-H dataset", defaultValue = "0.1")
  private double scale;

  @CommandLine.Option(names = {"--queries"}, description = "Set a comma-separated list of query IDs to run, run all queries if not specified. Example: --queries=q1,q6", split = ",")
  private String[] queries = new String[0];

  @CommandLine.Option(names = {"--log-level"}, description = "Set log level: 0 for DEBUG, 1 for INFO, 2 for WARN", defaultValue = "2")
  private int logLevel;

  @CommandLine.Option(names = {"--explain"}, description = "Output explain result for queries", defaultValue = "false")
  private boolean explain;

  @CommandLine.Option(names = {"--error-on-memleak"}, description = "Fail the test when memory leak is detected by Spark's memory manager", defaultValue = "false")
  private boolean errorOnMemLeak;

  @CommandLine.Option(names = {"--enable-ui"}, description = "Enable Spark UI", defaultValue = "false")
  private boolean enableUi;

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

  @CommandLine.Option(names = {"--shuffle-partitions"}, description = "Generate data with partitions", defaultValue = "100")
  private int shufflePartitions;

  @CommandLine.Option(names = {"--min-scan-partitions"}, description = "Use minimum number of partitions to read data", defaultValue = "false")
  private boolean minimumScanPartitions;

  @CommandLine.Option(names = {"--gen-partitioned-data"}, description = "Generate data with partitions", defaultValue = "false")
  private boolean genPartitionedData;

  @CommandLine.Option(names = {"--extra-conf"}, description = "Extra Spark config entries applying to generated Spark session. E.g. --conf=k1=v1 --conf=k2=v2")
  private Map<String, String> extraSparkConf = Collections.emptyMap();

  public Tpc() {
  }

  private SparkConf pickSparkConf(String preset) {
    SparkConf conf;
    switch (preset) {
      case "vanilla":
        conf = Constants.VANILLA_CONF();
        break;
      case "velox":
        conf = Constants.VELOX_CONF();
        break;
      case "velox-with-celeborn":
        conf = Constants.VELOX_WITH_CELEBORN_CONF();
        break;
      default:
        throw new IllegalArgumentException("Preset not found: " + preset);
    }
    return conf;
  }

  @Override
  public Integer call() throws Exception {
    final SparkConf baselineConf = pickSparkConf(baselinePreset);
    final SparkConf testConf = pickSparkConf(preset);
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

    final Action[] actions =
        Actions.createActions(mode, skipDataGen, scale, genPartitionedData, queries, explain, iterations);

    scala.collection.immutable.Map<String, String> extraSparkConfScala =
        JavaConverters.mapAsScalaMapConverter(extraSparkConf).asScala().toMap(
            Predef.conforms());

    final TpcSuite suite;
    switch (benchmarkType) {
      case "h":
        suite = new TpchSuite(actions, testConf, baselineConf,
            extraSparkConfScala, level, errorOnMemLeak, enableUi,
            enableHsUi, hsUiPort, cpus, offHeapSize, disableAqe, disableBhj,
            disableWscg, shufflePartitions, minimumScanPartitions);
        break;
      case "ds":
        suite = new TpcdsSuite(actions, testConf, baselineConf,
                extraSparkConfScala, level, errorOnMemLeak, enableUi,
            enableHsUi, hsUiPort, cpus, offHeapSize, disableAqe, disableBhj,
            disableWscg, shufflePartitions, minimumScanPartitions);
        break;
      default:
        throw new IllegalArgumentException("TPC benchmark type not found: " + benchmarkType);
    }
    final boolean succeed;
    try {
      succeed = suite.run();
    } finally {
      suite.close();
    }
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
