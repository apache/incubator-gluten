package io.glutenproject.substrait.expression;

import io.substrait.proto.Expression;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class StringMapNode implements ExpressionNode, Serializable {
  private final Map<String, String> values = new HashMap();

  public StringMapNode(Map<String, String> values) {
    this.values.putAll(values);
  }

  @Override
  public Expression toProtobuf() {
    Expression.Literal.Builder literalBuilder = Expression.Literal.newBuilder();
    Expression.Literal.Map.KeyValue.Builder keyValueBuilder =
        Expression.Literal.Map.KeyValue.newBuilder();
    Expression.Literal.Map.Builder mapBuilder = Expression.Literal.Map.newBuilder();
    for (Map.Entry<String, String> entry : values.entrySet()) {
      literalBuilder.setString(entry.getKey());
      keyValueBuilder.setKey(literalBuilder.build());
      literalBuilder.setString(entry.getValue());
      keyValueBuilder.setValue(literalBuilder.build());
      mapBuilder.addKeyValues(keyValueBuilder.build());
    }
    literalBuilder.setMap(mapBuilder.build());

    Expression.Builder builder = Expression.newBuilder();
    builder.setLiteral(literalBuilder.build());

    return builder.build();
  }
}
