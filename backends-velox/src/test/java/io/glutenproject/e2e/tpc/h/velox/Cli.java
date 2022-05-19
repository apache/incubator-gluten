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

package io.glutenproject.e2e.tpc.h.velox;

import io.glutenproject.e2e.tpc.TpcRunner;
import io.glutenproject.e2e.tpc.TpcRunner$;
import io.glutenproject.e2e.tpc.h.TpchDataGen;
import io.glutenproject.e2e.tpc.h.TypeModifier;
import org.apache.spark.sql.GlutenSparkSessionSwitcher;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "gluten-velox-cli", mixinStandardHelpOptions = true,
  description = "A CLI allowing running sample queries within Gluten + Velox backend")
public class Cli implements Callable<Integer> {
  private GlutenSparkSessionSwitcher switcher;

  @CommandLine.Option(required = true, names = {"-b", "--backend-type"}, description = "Backend used: velox, gazelle-cpp, vanilla, ...")
  private String backendType;

  @CommandLine.Option(names = {"-s", "--scale"}, description = "The scale factor of sample TPC-H dataset", defaultValue = "0.1")
  private double scale;

  @CommandLine.Option(names = {"--integer-as-double"}, description = "Generate integer as double", defaultValue = "false")
  private boolean integerAsDouble;

  @CommandLine.Option(names = {"--long-as-double"}, description = "Generate long as double", defaultValue = "false")
  private boolean longAsDouble;

  @CommandLine.Option(names = {"--date-as-double"}, description = "Generate date as double", defaultValue = "false")
  private boolean dateAsDouble;

  @Override
  public Integer call() {
    switcher = new GlutenSparkSessionSwitcher();
    switcher.registerSession("vanilla", io.glutenproject.e2e.tpc.h.package$.MODULE$.VANILLA_CONF());
    switch (backendType) {
      case "vanilla":
        break;
      case "velox":
        switcher.registerSession("velox", package$.MODULE$.VELOX_BACKEND_CONF());
        break;
      case "gazelle-cpp":
        switcher.registerSession("gazelle-cpp", package$.MODULE$.GAZELLE_CPP_BACKEND_CONF());
        break;
      default:
        throw new IllegalArgumentException("Backend type not found: " + backendType);
    }
    switcher.useSession("vanilla");
    final List<TypeModifier> typeModifiers = new ArrayList<>();
    if (integerAsDouble) {
      typeModifiers.add(package$.MODULE$.TYPE_MODIFIER_INTEGER_AS_DOUBLE());
    }
    if (longAsDouble) {
      typeModifiers.add(package$.MODULE$.TYPE_MODIFIER_LONG_AS_DOUBLE());
    }
    if (dateAsDouble) {
      typeModifiers.add(package$.MODULE$.TYPE_MODIFIER_DATE_AS_DOUBLE());
    }
    String dataPath = "/tmp/tpch-generated";
    TpchDataGen dataGen = new TpchDataGen(switcher.spark(), scale, dataPath,
      typeModifiers.toArray(new TypeModifier[0]));
    dataGen.gen();
    switcher.useSession(backendType);
    TpcRunner$.MODULE$.createTables(switcher.spark(), dataPath);
    System.out.println("=============================");
    Scanner scanner = new Scanner(System.in);
    while (!Thread.currentThread().isInterrupted()) {
      try {
        System.out.print("> ");
        final String line = scanner.nextLine();
        if (Objects.equals(line, "exit")) {
          break;
        }
        switcher.spark().sql(line).show(100, false);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return 0;
  }

  public static void main(String... args) {
    int exitCode = new CommandLine(new Cli()).execute(args);
    System.exit(exitCode);
  }
}
