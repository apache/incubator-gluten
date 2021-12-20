package com.intel.oap.substrait.type;

import io.substrait.Type;

import java.io.Serializable;
import java.util.ArrayList;

public class StructNode implements TypeNode, Serializable {
    private final ArrayList<TypeNode> types = new ArrayList<>();

    StructNode(ArrayList<TypeNode> types) {
        this.types.addAll(types);
    }

    @Override
    public Type toProtobuf() {
        Type.Struct.Builder structBuilder = Type.Struct.newBuilder();
        for (TypeNode typeNode : types) {
            structBuilder.addTypes(typeNode.toProtobuf());
        }
        Type.Builder builder = Type.newBuilder();
        builder.setStruct(structBuilder.build());
        return builder.build();
    }
}
