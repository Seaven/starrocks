package com.starrocks.qe.recursivecte;

import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRelation;

import java.util.Stack;

public class RecursiveCTEAstCheck extends AstTraverser<Void, Void> {
    private final Stack<String> cteNameStack = new Stack<>();

    private boolean isRecursiveCte = false;

    public static boolean hasRecursiveCte(StatementBase stmt) {
        RecursiveCTEAstCheck check = new RecursiveCTEAstCheck();
        stmt.accept(check, null);
        return check.isRecursiveCte;
    }

    @Override
    public Void visitCTE(CTERelation node, Void context) {
        if (node.isAnchor()) {
            cteNameStack.push(node.getName());
        }
        super.visitCTE(node, context);
        cteNameStack.pop();
        return null;
    }

    @Override
    public Void visitTable(TableRelation node, Void context) {
        if (cteNameStack.isEmpty()) {
            return null;
        }
        if (node.getName().getDb() == null && node.getName().getTbl().equalsIgnoreCase(cteNameStack.peek())) {
            if (cteNameStack.size() != 1) {
                throw new SemanticException("Doesn't support multi-level recursive CTE");
            }
            isRecursiveCte = true;
        } else if (isRecursiveCte) {
            throw new SemanticException("Recursive CTE can only reference itself");
        }
        return null;
    }
}
