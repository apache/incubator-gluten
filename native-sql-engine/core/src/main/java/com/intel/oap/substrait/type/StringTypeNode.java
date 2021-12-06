package com.intel.oap.substrait.type;

import io.substrait.Type;

import java.io.Serializable;

public class StringTypeNode implements TypeNode, Serializable {
    private final String name;
    private final Boolean nullable;

    StringTypeNode(String name, Boolean nullable) {
        this.name = name;
        this.nullable = nullable;
    }

    @Override
    public Type toProtobuf() {
        Type.Variation.Builder variationBuilder = Type.Variation.newBuilder();
        variationBuilder.setName(name);

        Type.String.Builder stringBuilder = Type.String.newBuilder();
        stringBuilder.setVariation(variationBuilder.build());
        if (nullable) {
            stringBuilder.setNullability(Type.Nullability.NULLABLE);
        } else {
            stringBuilder.setNullability(Type.Nullability.REQUIRED);
        }
        Type.Builder builder = Type.newBuilder();
        builder.setString(stringBuilder.build());
        return builder.build();
    }
}
