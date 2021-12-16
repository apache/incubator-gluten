package com.intel.oap.substrait.rel;

import com.intel.oap.substrait.expression.ExpressionNode;
import com.intel.oap.substrait.type.TypeNode;
import io.substrait.ReadRel;
import io.substrait.Rel;
import io.substrait.Type;

import java.io.Serializable;
import java.util.ArrayList;

public class ReadRelNode implements RelNode, Serializable {
    private final ArrayList<TypeNode> types = new ArrayList<>();
    private final ArrayList<String> names = new ArrayList<>();
    private final ExpressionNode filterNode;
    private final LocalFilesNode partNode;

    ReadRelNode(ArrayList<TypeNode> types, ArrayList<String> names,
               ExpressionNode filterNode) {
        this.types.addAll(types);
        this.names.addAll(names);
        this.filterNode = filterNode;
        this.partNode = null;
    }

    ReadRelNode(ArrayList<TypeNode> types, ArrayList<String> names,
                ExpressionNode filterNode, LocalFilesNode partNode) {
        this.types.addAll(types);
        this.names.addAll(names);
        this.filterNode = filterNode;
        this.partNode = partNode;
    }

    @Override
    public Rel toProtobuf() {
        Type.Struct.Builder structBuilder = Type.Struct.newBuilder();
        for (TypeNode typeNode : types) {
            structBuilder.addTypes(typeNode.toProtobuf());
        }
        Type.NamedStruct.Builder nStructBuilder = Type.NamedStruct.newBuilder();
        nStructBuilder.setStruct(structBuilder.build());
        for (String name : names) {
            nStructBuilder.addNames(name);
        }
        ReadRel.Builder readBuilder = ReadRel.newBuilder();
        readBuilder.setBaseSchema(nStructBuilder.build());
        readBuilder.setFilter(filterNode.toProtobuf());
        if (partNode != null) {
            readBuilder.setLocalFiles(partNode.toProtobuf());
        }
        Rel.Builder builder = Rel.newBuilder();
        builder.setRead(readBuilder.build());
        return builder.build();
    }
}
