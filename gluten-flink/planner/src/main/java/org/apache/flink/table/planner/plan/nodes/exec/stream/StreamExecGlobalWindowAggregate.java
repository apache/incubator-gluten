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
package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.gluten.rexnode.AggregateCallConverter;
import org.apache.gluten.rexnode.Utils;
import org.apache.gluten.rexnode.WindowUtils;
import org.apache.gluten.table.runtime.operators.GlutenVectorOneInputOperator;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.PlanNodeIdGenerator;

import io.github.zhztheplayer.velox4j.aggregate.Aggregate;
import io.github.zhztheplayer.velox4j.aggregate.AggregateStep;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.plan.AggregationNode;
import io.github.zhztheplayer.velox4j.plan.EmptyNode;
import io.github.zhztheplayer.velox4j.plan.HashPartitionFunctionSpec;
import io.github.zhztheplayer.velox4j.plan.PartitionFunctionSpec;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.StreamWindowAggregationNode;
import io.github.zhztheplayer.velox4j.plan.StreamWindowPartitionFunctionSpec;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.runtime.groupwindow.WindowProperty;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigner;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.math3.util.ArithmeticUtils;

import javax.annotation.Nullable;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Stream {@link ExecNode} for window table-valued based global aggregate. */
@ExecNodeMetadata(
    name = "stream-exec-global-window-aggregate",
    version = 1,
    consumedOptions = "table.local-time-zone",
    producedTransformations =
        StreamExecGlobalWindowAggregate.GLOBAL_WINDOW_AGGREGATE_TRANSFORMATION,
    minPlanVersion = FlinkVersion.v1_15,
    minStateVersion = FlinkVersion.v1_15)
public class StreamExecGlobalWindowAggregate extends StreamExecWindowAggregateBase {

  public static final String GLOBAL_WINDOW_AGGREGATE_TRANSFORMATION = "global-window-aggregate";

  public static final String FIELD_NAME_LOCAL_AGG_INPUT_ROW_TYPE = "localAggInputRowType";

  @JsonProperty(FIELD_NAME_GROUPING)
  private final int[] grouping;

  @JsonProperty(FIELD_NAME_AGG_CALLS)
  private final AggregateCall[] aggCalls;

  @JsonProperty(FIELD_NAME_WINDOWING)
  private final WindowingStrategy windowing;

  @JsonProperty(FIELD_NAME_NAMED_WINDOW_PROPERTIES)
  private final NamedWindowProperty[] namedWindowProperties;

  /** The input row type of this node's local agg. */
  @JsonProperty(FIELD_NAME_LOCAL_AGG_INPUT_ROW_TYPE)
  private final RowType localAggInputRowType;

  @JsonProperty(FIELD_NAME_NEED_RETRACTION)
  private final boolean needRetraction;

  public StreamExecGlobalWindowAggregate(
      ReadableConfig tableConfig,
      int[] grouping,
      AggregateCall[] aggCalls,
      WindowingStrategy windowing,
      NamedWindowProperty[] namedWindowProperties,
      Boolean needRetraction,
      InputProperty inputProperty,
      RowType localAggInputRowType,
      RowType outputType,
      String description) {
    this(
        ExecNodeContext.newNodeId(),
        ExecNodeContext.newContext(StreamExecGlobalWindowAggregate.class),
        ExecNodeContext.newPersistedConfig(StreamExecGlobalWindowAggregate.class, tableConfig),
        grouping,
        aggCalls,
        windowing,
        namedWindowProperties,
        needRetraction,
        Collections.singletonList(inputProperty),
        localAggInputRowType,
        outputType,
        description);
  }

  @JsonCreator
  public StreamExecGlobalWindowAggregate(
      @JsonProperty(FIELD_NAME_ID) int id,
      @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
      @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
      @JsonProperty(FIELD_NAME_GROUPING) int[] grouping,
      @JsonProperty(FIELD_NAME_AGG_CALLS) AggregateCall[] aggCalls,
      @JsonProperty(FIELD_NAME_WINDOWING) WindowingStrategy windowing,
      @JsonProperty(FIELD_NAME_NAMED_WINDOW_PROPERTIES) NamedWindowProperty[] namedWindowProperties,
      @Nullable @JsonProperty(FIELD_NAME_NEED_RETRACTION) Boolean needRetraction,
      @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
      @JsonProperty(FIELD_NAME_LOCAL_AGG_INPUT_ROW_TYPE) RowType localAggInputRowType,
      @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
      @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
    super(id, context, persistedConfig, inputProperties, outputType, description);
    this.grouping = checkNotNull(grouping);
    this.aggCalls = checkNotNull(aggCalls);
    this.windowing = checkNotNull(windowing);
    this.namedWindowProperties = checkNotNull(namedWindowProperties);
    this.localAggInputRowType = localAggInputRowType;
    this.needRetraction = Optional.ofNullable(needRetraction).orElse(false);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Transformation<RowData> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    final ExecEdge inputEdge = getInputEdges().get(0);
    final Transformation<RowData> inputTransform =
        (Transformation<RowData>) inputEdge.translateToPlan(planner);
    final RowType inputRowType = (RowType) inputEdge.getOutputType();

    final ZoneId shiftTimeZone =
        TimeWindowUtil.getShiftTimeZone(
            windowing.getTimeAttributeType(), TableConfigUtils.getLocalTimeZone(config));
    // --- Begin Gluten-specific code changes ---
    // TODO: velox window not equal to flink window.
    io.github.zhztheplayer.velox4j.type.RowType inputType =
        (io.github.zhztheplayer.velox4j.type.RowType) LogicalTypeConverter.toVLType(inputRowType);
    io.github.zhztheplayer.velox4j.type.RowType outputType =
        (io.github.zhztheplayer.velox4j.type.RowType)
            LogicalTypeConverter.toVLType(getOutputType());
    List<FieldAccessTypedExpr> groupingKeys = Utils.generateFieldAccesses(inputType, grouping);
    List<Aggregate> aggregates = AggregateCallConverter.toAggregates(aggCalls, inputType);
    checkArgument(outputType.getNames().size() >= grouping.length + aggCalls.length);
    List<String> aggNames =
        outputType.getNames().stream()
            .skip(grouping.length)
            .limit(aggCalls.length)
            .collect(Collectors.toList());
    List<Integer> keyIndexes = Arrays.stream(grouping).boxed().collect(Collectors.toList());
    PartitionFunctionSpec keySelectorSpec = new HashPartitionFunctionSpec(inputType, keyIndexes);
    // TODO: support more window types.
    Tuple5<Long, Long, Long, Integer, Integer> windowSpecParams =
        WindowUtils.extractWindowParameters(windowing);
    long size = windowSpecParams.f0;
    long slide = windowSpecParams.f1;
    long offset = windowSpecParams.f2;
    int rowtimeIndex = windowSpecParams.f3;
    int windowType = windowSpecParams.f4;
    PartitionFunctionSpec sliceAssignerSpec =
        new StreamWindowPartitionFunctionSpec(
            inputType, rowtimeIndex, size, slide, offset, windowType);
    PlanNode aggregation =
        new AggregationNode(
            PlanNodeIdGenerator.newId(),
            AggregateStep.SINGLE,
            groupingKeys,
            groupingKeys,
            aggNames,
            aggregates,
            false,
            List.of(new EmptyNode(inputType)),
            null,
            List.of());
    PlanNode localAgg =
        new AggregationNode(
            PlanNodeIdGenerator.newId(),
            AggregateStep.SINGLE,
            groupingKeys,
            groupingKeys,
            aggNames,
            aggregates,
            false,
            List.of(new EmptyNode(inputType)),
            null,
            List.of());
    PlanNode windowAgg =
        new StreamWindowAggregationNode(
            PlanNodeIdGenerator.newId(),
            aggregation,
            localAgg,
            keySelectorSpec,
            sliceAssignerSpec,
            ArithmeticUtils.gcd(size, slide),
            TimeZone.getTimeZone(shiftTimeZone).useDaylightTime(),
            false,
            size,
            slide,
            offset,
            windowType,
            outputType);
    final OneInputStreamOperator windowOperator =
        new GlutenVectorOneInputOperator(
            new StatefulPlanNode(windowAgg.getId(), windowAgg),
            PlanNodeIdGenerator.newId(),
            inputType,
            Map.of(windowAgg.getId(), outputType));
    // --- End Gluten-specific code changes ---

    final RowDataKeySelector selector =
        KeySelectorUtil.getRowDataSelector(
            planner.getFlinkContext().getClassLoader(),
            grouping,
            InternalTypeInfo.of(inputRowType));

    final OneInputTransformation<RowData, RowData> transform =
        ExecNodeUtil.createOneInputTransformation(
            inputTransform,
            createTransformationMeta(GLOBAL_WINDOW_AGGREGATE_TRANSFORMATION, config),
            SimpleOperatorFactory.of(windowOperator),
            InternalTypeInfo.of(getOutputType()),
            inputTransform.getParallelism(),
            WINDOW_AGG_MEMORY_RATIO,
            false);

    // set KeyType and Selector for state
    transform.setStateKeySelector(selector);
    transform.setStateKeyType(selector.getProducedType());
    return transform;
  }

  private GeneratedNamespaceAggsHandleFunction<Long> createAggsHandler(
      String name,
      SliceAssigner sliceAssigner,
      AggregateInfoList aggInfoList,
      int mergedAccOffset,
      boolean mergedAccIsOnHeap,
      DataType[] mergedAccExternalTypes,
      ExecNodeConfig config,
      ClassLoader classLoader,
      RelBuilder relBuilder,
      ZoneId shifTimeZone) {
    final AggsHandlerCodeGenerator generator =
        new AggsHandlerCodeGenerator(
                new CodeGeneratorContext(config, classLoader),
                relBuilder,
                JavaScalaConversionUtil.toScala(localAggInputRowType.getChildren()),
                true) // copyInputField
            .needAccumulate()
            .needMerge(mergedAccOffset, mergedAccIsOnHeap, mergedAccExternalTypes);

    final List<WindowProperty> windowProperties =
        Arrays.asList(
            Arrays.stream(namedWindowProperties)
                .map(NamedWindowProperty::getProperty)
                .toArray(WindowProperty[]::new));

    return generator.generateNamespaceAggsHandler(
        name,
        aggInfoList,
        JavaScalaConversionUtil.toScala(windowProperties),
        sliceAssigner,
        // we use window end timestamp to indicate a slicing window, see SliceAssigner
        Long.class,
        shifTimeZone);
  }
}
