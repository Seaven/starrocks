package com.starrocks.sql.formatter;

import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;

import java.util.stream.Collectors;

public class ExprVerboseVisitor extends ExprExplainVisitor {
    @Override
    public String visitFunctionCall(FunctionCallExpr node, Void context) {
        StringBuilder sb = new StringBuilder();

        sb.append(node.getFnName());
        sb.append("[");
        sb.append("(");

        if (node.getFnParams().isStar()) {
            sb.append("*");
        }
        if (node.isDistinct()) {
            sb.append("DISTINCT ");
        }

        String childrenSql = node.getChildren().stream()
                .limit(node.getChildren().size() - node.getFnParams().getOrderByElemNum())
                .map(c -> visit(c, context))
                .collect(Collectors.joining(", "));
        sb.append(childrenSql);

        if (node.getFnParams().getOrderByElements() != null) {
            sb.append(node.getFnParams().getOrderByStringToSql());
        }
        sb.append(")");

        if (node.getFn() != null) {
            sb.append(" args: ");
            for (int i = 0; i < node.getFn().getArgs().length; ++i) {
                if (i != 0) {
                    sb.append(',');
                }
                sb.append(node.getFn().getArgs()[i].getPrimitiveType().toString());
            }
            sb.append(";");
            sb.append(" result: ").append(node.getType()).append(";");
        }
        sb.append(" args nullable: ").append(node.hasNullableChild()).append(";");
        sb.append(" result nullable: ").append(node.isNullable());
        sb.append("]");
        return sb.toString();
    }

    @Override
    public String visitCastExpr(CastExpr node, Void context) {
        if (node.isNoOp()) {
            return node.getChild(0).accept(this, context);
        } else {
            return "cast(" + node.getChild(0).accept(this, context) + " AS " + node.getType() + ")";
        }
    }

    @Override
    public String visitSlot(SlotRef node, Void context) {
        if (node.getLabel() != null) {
            return "[" + node.getLabel() + "," +
                    " " + node.getDesc().getType() + "," +
                    " " + node.getDesc().getIsNullable() + "]";
        } else {
            return "[" + node.getDesc().getId().asInt() + "," +
                    " " + node.getDesc().getType() + "," +
                    " " + node.getDesc().getIsNullable() + "]";
        }
    }
}
