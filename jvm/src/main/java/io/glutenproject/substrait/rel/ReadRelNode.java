package io.glutenproject.substrait.rel;

import io.glutenproject.substrait.SubstraitContext;
import io.glutenproject.substrait.expression.ExpressionNode;
import io.glutenproject.substrait.type.TypeNode;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;
import io.substrait.proto.Type;

import java.io.Serializable;
import java.util.ArrayList;

public class ReadRelNode implements RelNode, Serializable {
  private final ArrayList<TypeNode> types = new ArrayList<>();
  private final ArrayList<String> names = new ArrayList<>();
  private final SubstraitContext context;
  private final ExpressionNode filterNode;
  private final Long iteratorIndex;

  ReadRelNode(ArrayList<TypeNode> types, ArrayList<String> names,
              SubstraitContext context, ExpressionNode filterNode, Long iteratorIndex) {
    this.types.addAll(types);
    this.names.addAll(names);
    this.context = context;
    this.filterNode = filterNode;
    this.iteratorIndex = iteratorIndex;
  }

  @Override
  public Rel toProtobuf() {
    RelCommon.Builder relCommonBuilder = RelCommon.newBuilder();
    relCommonBuilder.setDirect(RelCommon.Direct.newBuilder());

    Type.Struct.Builder structBuilder = Type.Struct.newBuilder();
    for (TypeNode typeNode : types) {
      structBuilder.addTypes(typeNode.toProtobuf());
    }
    NamedStruct.Builder nStructBuilder = NamedStruct.newBuilder();
    nStructBuilder.setStruct(structBuilder.build());
    for (String name : names) {
      nStructBuilder.addNames(name);
    }
    ReadRel.Builder readBuilder = ReadRel.newBuilder();
    readBuilder.setCommon(relCommonBuilder.build());
    readBuilder.setBaseSchema(nStructBuilder.build());
    if (filterNode != null) {
      readBuilder.setFilter(filterNode.toProtobuf());
    }
    if (this.iteratorIndex != null) {
      readBuilder.setLocalFiles(context.getInputIteratorNode(iteratorIndex).toProtobuf());
    } else if (context.getLocalFilesNodes() != null && !context.getLocalFilesNodes().isEmpty()) {
      Serializable currentLocalFileNode = context.getCurrentLocalFileNode();
      if (currentLocalFileNode instanceof LocalFilesNode) {
        readBuilder.setLocalFiles(((LocalFilesNode)currentLocalFileNode).toProtobuf());
      } else if (currentLocalFileNode instanceof ExtensionTableNode) {
        readBuilder.setExtensionTable(((ExtensionTableNode)currentLocalFileNode).toProtobuf());
      }
    }
    Rel.Builder builder = Rel.newBuilder();
    builder.setRead(readBuilder.build());
    return builder.build();
  }
}
