/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

grammar AsterixInternalLanguage;

@parser::header
{
// DO NOT MODIFY - generated from AsterixInternalLanguage.g4 using "mx create-sl-parser"
package org.apache.asterix.codegen.truffle.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.oracle.truffle.api.source.Source;
import com.oracle.truffle.api.RootCallTarget;
import org.apache.asterix.codegen.truffle.AILLanguage;
import org.apache.asterix.codegen.truffle.nodes.AILExpressionNode;
import org.apache.asterix.codegen.truffle.nodes.AILRootNode;
import org.apache.asterix.codegen.truffle.nodes.AILStatementNode;
import org.apache.asterix.codegen.truffle.parser.AILParseError;
}

@lexer::header
{
// DO NOT MODIFY - generated from AsterixInternalLanguage.g4 using "mx create-sl-parser"
package org.apache.asterix.codegen.truffle.parser;
}

@parser::members
{
private AILNodeFactory factory;
private Source source;

private static final class BailoutErrorListener extends BaseErrorListener {
    private final Source source;
    BailoutErrorListener(Source source) {
        this.source = source;
    }
    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
        throwParseError(source, line, charPositionInLine, (Token) offendingSymbol, msg);
    }
}

public void SemErr(Token token, String message) {
    assert token != null;
    throwParseError(source, token.getLine(), token.getCharPositionInLine(), token, message);
}

private static void throwParseError(Source source, int line, int charPositionInLine, Token token, String message) {
    int col = charPositionInLine + 1;
    String location = "-- line " + line + " col " + col + ": ";
    int length = token == null ? 1 : Math.max(token.getStopIndex() - token.getStartIndex(), 0);
    throw new AILParseError(source, line, col, length, String.format("Error(s) parsing script:%n" + location + message));
}

public static Map<String, RootCallTarget> parseAIL(AILLanguage language, Source source) {
    AsterixInternalLanguageLexer lexer = new AsterixInternalLanguageLexer(CharStreams.fromString(source.getCharacters().toString()));
    AsterixInternalLanguageParser parser = new AsterixInternalLanguageParser(new CommonTokenStream(lexer));
    lexer.removeErrorListeners();
    parser.removeErrorListeners();
    BailoutErrorListener listener = new BailoutErrorListener(source);
    lexer.addErrorListener(listener);
    parser.addErrorListener(listener);
    parser.factory = new AILNodeFactory(language, source);
    parser.source = source;
    parser.asterixinternallanguage();
    return parser.factory.getAllFunctions();
}
}

// parser




asterixinternallanguage
:
function function* EOF
;


function
:
'function'
IDENTIFIER
s='('
                                                { factory.startFunction($IDENTIFIER, $s); }
(
    IDENTIFIER                                  { factory.addFormalParameter($IDENTIFIER); }
    (
        ','
        IDENTIFIER                              { factory.addFormalParameter($IDENTIFIER); }
    )*
)?
')'
body=block[false]                               { factory.finishFunction($body.result); }
;



block [boolean inLoop] returns [AILStatementNode result]
:                                               { factory.startBlock();
                                                  List<AILStatementNode> body = new ArrayList<>(); }
s='{'
(
    statement[inLoop]                           { body.add($statement.result); }
)*
e='}'
                                                { $result = factory.finishBlock(body, $s.getStartIndex(), $e.getStopIndex() - $s.getStartIndex() + 1); }
;


statement [boolean inLoop] returns [AILStatementNode result]
:
(
    while_statement                             { $result = $while_statement.result; }
|
    b='break'                                   { if (inLoop) { $result = factory.createBreak($b); } else { SemErr($b, "break used outside of loop"); } }
    ';'
|
    c='continue'                                { if (inLoop) { $result = factory.createContinue($c); } else { SemErr($c, "continue used outside of loop"); } }
    ';'
|
    if_statement[inLoop]                        { $result = $if_statement.result; }
|
    return_statement                            { $result = $return_statement.result; }
|
    expression ';'                              { $result = $expression.result; }
|
    d='debugger'                                { $result = factory.createDebugger($d); }
    ';'
)
;


while_statement returns [AILStatementNode result]
:
w='while'
'('
condition=expression
')'
body=block[true]                                { $result = factory.createWhile($w, $condition.result, $body.result); }
;


if_statement [boolean inLoop] returns [AILStatementNode result]
:
i='if'
'('
condition=expression
')'
then=block[inLoop]                              { AILStatementNode elsePart = null; }
(
    'else'
    block[inLoop]                               { elsePart = $block.result; }
)?                                              { $result = factory.createIf($i, $condition.result, $then.result, elsePart); }
;


return_statement returns [AILStatementNode result]
:
r='return'                                      { AILExpressionNode value = null; }
(
    expression                                  { value = $expression.result; }
)?                                              { $result = factory.createReturn($r, value); }
';'
;


expression returns [AILExpressionNode result]
:
logic_term                                      { $result = $logic_term.result; }
(
    op='||'
    logic_term                                  { $result = factory.createBinary($op, $result, $logic_term.result); }
)*
;


logic_term returns [AILExpressionNode result]
:
logic_factor                                    { $result = $logic_factor.result; }
(
    op='&&'
    logic_factor                                { $result = factory.createBinary($op, $result, $logic_factor.result); }
)*
;


logic_factor returns [AILExpressionNode result]
:
arithmetic                                      { $result = $arithmetic.result; }
(
    op=('<' | '<=' | '>' | '>=' | '==' | '!=' )
    arithmetic                                  { $result = factory.createBinary($op, $result, $arithmetic.result); }
)?
;


arithmetic returns [AILExpressionNode result]
:
term                                            { $result = $term.result; }
(
    op=('+' | '++' | '-')
    term                                        { $result = factory.createBinary($op, $result, $term.result); }
)*
;


term returns [AILExpressionNode result]
:
factor                                          { $result = $factor.result; }
(
    op=('*' | '/' | '/\\' | '\\/' | '%')
    factor                                      { $result = factory.createBinary($op, $result, $factor.result); }
)*
;


factor returns [AILExpressionNode result]
:
(
    TRUE                                        { $result = factory.createBooleanLiteral($TRUE); }
|
    FALSE                                       { $result = factory.createBooleanLiteral($FALSE); }
|
    MISSING                                     { $result = factory.createMissingLiteral($MISSING); }
|
    NULL                                        { $result = factory.createNullLiteral($NULL); }
|
    IDENTIFIER                                  { AILExpressionNode assignmentName = factory.createStringLiteral($IDENTIFIER, false); }
    (
        member_expression[null, null, assignmentName] { $result = $member_expression.result; }
    |
                                                { $result = factory.createRead(assignmentName); }
    )
|
    STRING_LITERAL                              { $result = factory.createStringLiteral($STRING_LITERAL, true); }
|
    STRING_RUNTIME_LITERAL                      { $result = factory.createStringRuntimeLiteral($STRING_RUNTIME_LITERAL, true); }
|
    NUMERIC_LITERAL                             { $result = factory.createNumericLiteral($NUMERIC_LITERAL); }
|
    DOUBLE_LITERAL                              { $result  = factory.createDoubleLiteral($DOUBLE_LITERAL); }
|
    s='('
    expr=expression
    e=')'                                       { $result = factory.createParenExpression($expr.result, $s.getStartIndex(), $e.getStopIndex() - $s.getStartIndex() + 1); }
|
    not='!'
    factor                                      { $result = factory.createNotExpression($factor.result, $not.getStartIndex()); }
|
    negation='-'
    factor                                      { $result = factory.createNegatedExpression($factor.result, $negation.getStartIndex()); }
)
;


member_expression [AILExpressionNode r, AILExpressionNode assignmentReceiver, AILExpressionNode assignmentName] returns [AILExpressionNode result]
:                                               { AILExpressionNode receiver = r;
                                                  AILExpressionNode nestedAssignmentName = null; }
(
    '('                                         { List<AILExpressionNode> parameters = new ArrayList<>();
                                                  if (receiver == null) {
                                                      receiver = factory.createRead(assignmentName);
                                                  } }
    (
        expression                              { parameters.add($expression.result); }
        (
            ','
            expression                          { parameters.add($expression.result); }
        )*
    )?
    e=')'
                                                { $result = factory.createCall(receiver, parameters, $e); }
|
    '='
    expression                                  { if (assignmentName == null) {
                                                      SemErr($expression.start, "invalid assignment target");
                                                  } else if (assignmentReceiver == null) {
                                                      $result = factory.createAssignment(assignmentName, $expression.result);
                                                  } else {
                                                      $result = factory.createWriteProperty(assignmentReceiver, assignmentName, $expression.result);
                                                  } }
|
    '.'                                         { if (receiver == null) {
                                                       receiver = factory.createRead(assignmentName);
                                                  } }
    IDENTIFIER
                                                { nestedAssignmentName = factory.createStringLiteral($IDENTIFIER, false);
                                                  $result = factory.createReadProperty(receiver, nestedAssignmentName); }
|
    '['                                         { if (receiver == null) {
                                                      receiver = factory.createRead(assignmentName);
                                                  } }
    expression
                                                { nestedAssignmentName = $expression.result;
                                                  $result = factory.createReadProperty(receiver, nestedAssignmentName); }
    ']'
)
(
    member_expression[$result, receiver, nestedAssignmentName] { $result = $member_expression.result; }
)?
;

// lexer

WS : [ \t\r\n\u000C]+ -> skip;
COMMENT : '/*' .*? '*/' -> skip;
LINE_COMMENT : '//' ~[\r\n]* -> skip;

fragment LETTER : [A-Z] | [a-z] | '_' | '$';
fragment NON_ZERO_DIGIT : [1-9];
fragment DIGIT : [0-9];
fragment HEX_DIGIT : [0-9] | [a-f] | [A-F];
fragment OCT_DIGIT : [0-7];
fragment BINARY_DIGIT : '0' | '1';
fragment TAB : '\t';
fragment STRING_CHAR : ~('`' | '"' | '\\' | '\r' | '\n');

TRUE : 'true';
FALSE: 'false';
MISSING: 'MISSING'; // Asterix's MISSING
NULL: 'NULL'; // Asterix's NULL - not AIL/JAVA null
IDENTIFIER : LETTER (LETTER | DIGIT)*;
STRING_LITERAL : '"' STRING_CHAR* '"';
STRING_RUNTIME_LITERAL : '`' STRING_CHAR* '`';
NUMERIC_LITERAL : '0' | NON_ZERO_DIGIT DIGIT*;
DOUBLE_LITERAL : NUMERIC_LITERAL '.' DIGIT+;

