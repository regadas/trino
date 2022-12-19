/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.deltalake.expression;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.HexFormat.isHexDigit;

public class SparkExpressionBuilder
        extends SparkExpressionBaseVisitor<Object>
{
    private static final char SPARK_ESCAPE_CHARACTER = '\\';

    @Override
    public Object visitStandaloneExpression(SparkExpressionParser.StandaloneExpressionContext context)
    {
        return visit(context.expression());
    }

    @Override
    public Object visitPredicated(SparkExpressionParser.PredicatedContext context)
    {
        // Handle comparison operator
        if (context.predicate() != null) {
            return visit(context.predicate());
        }

        // Handle simple expression likes just TRUE
        return visit(context.valueExpression);
    }

    @Override
    public Object visitComparison(SparkExpressionParser.ComparisonContext context)
    {
        return new ComparisonExpression(
                getComparisonOperator(((TerminalNode) context.comparisonOperator().getChild(0)).getSymbol()),
                (SparkExpression) visit(context.value),
                (SparkExpression) visit(context.right));
    }

    @Override
    public SparkExpression visitAnd(SparkExpressionParser.AndContext context)
    {
        verify(context.booleanExpression().size() == 2, "AND operator expects two expressions: " + context.booleanExpression());
        return new LogicalExpression(LogicalExpression.Operator.AND, visit(context.booleanExpression(0), SparkExpression.class), visit(context.booleanExpression(1), SparkExpression.class));
    }

    @Override
    public Object visitColumnReference(SparkExpressionParser.ColumnReferenceContext context)
    {
        return visit(context.identifier());
    }

    private static ComparisonExpression.Operator getComparisonOperator(Token symbol)
    {
        switch (symbol.getType()) {
            case SparkExpressionLexer.EQ:
                return ComparisonExpression.Operator.EQUAL;
            case SparkExpressionLexer.NEQ:
                return ComparisonExpression.Operator.NOT_EQUAL;
            case SparkExpressionLexer.LT:
                return ComparisonExpression.Operator.LESS_THAN;
            case SparkExpressionLexer.LTE:
                return ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
            case SparkExpressionLexer.GT:
                return ComparisonExpression.Operator.GREATER_THAN;
            case SparkExpressionLexer.GTE:
                return ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
        }

        throw new IllegalArgumentException("Unsupported operator: " + symbol.getText());
    }

    @Override
    public Object visitBooleanLiteral(SparkExpressionParser.BooleanLiteralContext context)
    {
        return new BooleanLiteral(context.getText());
    }

    @Override
    public SparkExpression visitIntegerLiteral(SparkExpressionParser.IntegerLiteralContext context)
    {
        return new LongLiteral(context.getText());
    }

    @Override
    public Object visitUnicodeStringLiteral(SparkExpressionParser.UnicodeStringLiteralContext context)
    {
        return new StringLiteral(decodeUnicodeLiteral(context));
    }

    private static String decodeUnicodeLiteral(SparkExpressionParser.UnicodeStringLiteralContext context)
    {
        String rawContent = unquote(context.getText());
        StringBuilder unicodeStringBuilder = new StringBuilder();
        StringBuilder escapedCharacterBuilder = new StringBuilder();
        int charactersNeeded = 0;
        UnicodeDecodeState state = UnicodeDecodeState.EMPTY;
        for (int i = 0; i < rawContent.length(); i++) {
            char ch = rawContent.charAt(i);
            switch (state) {
                case EMPTY:
                    if (ch == SPARK_ESCAPE_CHARACTER) {
                        state = UnicodeDecodeState.ESCAPED;
                    }
                    else {
                        unicodeStringBuilder.append(ch);
                    }
                    break;
                case ESCAPED:
                    if (ch == SPARK_ESCAPE_CHARACTER) {
                        unicodeStringBuilder.append(SPARK_ESCAPE_CHARACTER);
                        state = UnicodeDecodeState.EMPTY;
                    }
                    else if (ch == 'u') {
                        state = UnicodeDecodeState.UNICODE_SEQUENCE;
                        charactersNeeded = 4;
                    }
                    else if (ch == 'U') {
                        state = UnicodeDecodeState.UNICODE_SEQUENCE;
                        charactersNeeded = 8;
                    }
                    else if (isHexDigit(ch)) {
                        state = UnicodeDecodeState.UNICODE_SEQUENCE;
                        escapedCharacterBuilder.append(ch);
                    }
                    else {
                        throw new ParsingException("Invalid hexadecimal digit: " + ch);
                    }
                    break;
                case UNICODE_SEQUENCE:
                    checkState(isHexDigit(ch), "Incomplete escape sequence: " + escapedCharacterBuilder, context);
                    escapedCharacterBuilder.append(ch);
                    if (charactersNeeded == escapedCharacterBuilder.length()) {
                        String currentEscapedCode = escapedCharacterBuilder.toString();
                        escapedCharacterBuilder.setLength(0);
                        int codePoint = Integer.parseInt(currentEscapedCode, 16);
                        checkState(Character.isValidCodePoint(codePoint), "Invalid escaped character: " + currentEscapedCode, context);
                        if (Character.isSupplementaryCodePoint(codePoint)) {
                            unicodeStringBuilder.appendCodePoint(codePoint);
                        }
                        else {
                            char currentCodePoint = (char) codePoint;
                            checkState(!Character.isSurrogate(currentCodePoint), format("Invalid escaped character: %s. Escaped character is a surrogate. Use '\\+123456' instead.", currentEscapedCode), context);
                            unicodeStringBuilder.append(currentCodePoint);
                        }
                        state = UnicodeDecodeState.EMPTY;
                        charactersNeeded = -1;
                    }
                    else {
                        checkState(charactersNeeded > escapedCharacterBuilder.length(), "Unexpected escape sequence length: " + escapedCharacterBuilder.length(), context);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        checkState(state == UnicodeDecodeState.EMPTY, "Incomplete escape sequence: " + escapedCharacterBuilder, context);
        return unicodeStringBuilder.toString();
    }

    private static String unquote(String value)
    {
        if (value.startsWith("\"") && value.endsWith("\"")) {
            return value.substring(1, value.length() - 1)
                    .replace("\"\"", "\"");
        }
        if (value.startsWith("'") && value.endsWith("'")) {
            return value.substring(1, value.length() - 1)
                    .replace("''", "'");
        }
        return value;
    }

    private enum UnicodeDecodeState
    {
        EMPTY,
        ESCAPED,
        UNICODE_SEQUENCE
    }

    @Override
    public SparkExpression visitUnquotedIdentifier(SparkExpressionParser.UnquotedIdentifierContext context)
    {
        return new Identifier(context.getText());
    }

    @Override
    public Object visitBackQuotedIdentifier(SparkExpressionParser.BackQuotedIdentifierContext context)
    {
        String token = context.getText();
        String identifier = token.substring(1, token.length() - 1)
                .replace("``", "`");
        return new Identifier(identifier);
    }

    private <T> T visit(ParserRuleContext context, Class<T> expected)
    {
        return expected.cast(super.visit(context));
    }

    // default implementation is error-prone
    @Override
    protected Object aggregateResult(Object aggregate, Object nextResult)
    {
        if (nextResult == null) {
            throw new UnsupportedOperationException("not yet implemented");
        }
        if (aggregate == null) {
            return nextResult;
        }
        throw new UnsupportedOperationException(format("Cannot combine %s and %s", aggregate, nextResult));
    }
}
