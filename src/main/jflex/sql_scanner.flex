// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.mininglamp.analysis;

import java_cup.runtime.Symbol;
import java.io.StringWriter;
import java.lang.Integer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import com.mininglamp.analysis.SqlParserSymbols;
import com.mininglamp.common.util.SqlUtils;
import com.mininglamp.qe.SqlModeHelper;

%%

%class SqlScanner
%cup
%public
%final
%eofval{
    return newToken(SqlParserSymbols.EOF, null);
%eofval}
%unicode
%line
%column
%ctorarg Long sql_mode
%{
    // Help to judge a integer-literal is bigger than LARGEINT_MAX
    // NOTE: the 'longMin' is not '-2^63' here, to make sure the return value functions
    // like 'long abs(long)' is same with the type of arguments, which is valid.
    private static final BigInteger LONG_MAX = new BigInteger("9223372036854775807"); // 2^63 - 1

    private static final BigInteger LARGEINT_MAX_ABS = new BigInteger("170141183460469231731687303715884105728"); // 2^127

    // This param will affect the tokens returned by scanner.
    // For example:
    // In PIPES_AS_CONCAT_MODE(0x0002), scanner will return token with id as KW_PIPE instead of KW_OR when '||' is scanned.
    private long sql_mode;

    /**
       * Creates a new scanner to chain-call the generated constructor.
       * There is also a java.io.InputStream version of this constructor.
       * If you use this constructor, sql_mode will be set to 0 (default)
       *
       * @param   in  the java.io.Reader to read input from.
       */
    public SqlScanner(java.io.Reader in) {
      this(in, 0L);
    }

    /**
       * Creates a new scanner chain-call the generated constructor.
       * There is also java.io.Reader version of this constructor.
       * If you use this constructor, sql_mode will be set to 0 (default)
       *
       * @param   in  the java.io.Inputstream to read input from.
       */
    public SqlScanner(java.io.InputStream in) {
      this(in, 0L);
    }

    // map from keyword string to token id
    // we use a linked hash map because the insertion order is important.
    // for example, we want "and" to come after "&&" to make sure error reporting
    // uses "and" as a display name and not "&&"
    private static final Map<String, Integer> keywordMap = new LinkedHashMap<String, Integer>();
    static {
        keywordMap.put("show", new Integer(SqlParserSymbols.KW_SHOW));
        keywordMap.put("schemas", new Integer(SqlParserSymbols.KW_SCHEMAS));
        keywordMap.put("add", new Integer(SqlParserSymbols.KW_ADD));
        keywordMap.put("schema", new Integer(SqlParserSymbols.KW_SCHEMA));
        keywordMap.put("database", new Integer(SqlParserSymbols.KW_DATABASE));
        keywordMap.put("databases", new Integer(SqlParserSymbols.KW_DATABASES));
        keywordMap.put("select", new Integer(SqlParserSymbols.KW_SELECT));
        keywordMap.put("from", new Integer(SqlParserSymbols.KW_FROM));
        keywordMap.put("union", new Integer(SqlParserSymbols.KW_UNION));
        keywordMap.put("intersect", new Integer(SqlParserSymbols.KW_INTERSECT));
        keywordMap.put("except", new Integer(SqlParserSymbols.KW_EXCEPT));
        keywordMap.put("minus", new Integer(SqlParserSymbols.KW_MINUS));
        keywordMap.put("all", new Integer(SqlParserSymbols.KW_ALL));
        keywordMap.put("values", new Integer(SqlParserSymbols.KW_VALUES));
        keywordMap.put("view", new Integer(SqlParserSymbols.KW_VIEW));
        keywordMap.put("as", new Integer(SqlParserSymbols.KW_AS));
        keywordMap.put("table", new Integer(SqlParserSymbols.KW_TABLE));
        keywordMap.put("tables", new Integer(SqlParserSymbols.KW_TABLES));
        keywordMap.put("desc", new Integer(SqlParserSymbols.KW_DESCRIBE));
        keywordMap.put("drop", new Integer(SqlParserSymbols.KW_DROP));
        keywordMap.put("describe", new Integer(SqlParserSymbols.KW_DESCRIBE));
        keywordMap.put("create", new Integer(SqlParserSymbols.KW_CREATE));
        keywordMap.put("use", new Integer(SqlParserSymbols.KW_USE));
        keywordMap.put("or", new Integer(SqlParserSymbols.KW_OR));
        keywordMap.put("||", new Integer(SqlParserSymbols.KW_PIPE));
   }

  // map from token id to token description
  public static final Map<Integer, String> tokenIdMap =
      new HashMap<Integer, String>();
  static {
    Iterator<Map.Entry<String, Integer>> it = keywordMap.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, Integer> pairs = (Map.Entry<String, Integer>) it.next();
      tokenIdMap.put(pairs.getValue(), pairs.getKey().toUpperCase());
    }

    // add non-keyword tokens
    tokenIdMap.put(new Integer(SqlParserSymbols.IDENT), "IDENTIFIER");
    tokenIdMap.put(new Integer(SqlParserSymbols.COMMA), "COMMA");
    tokenIdMap.put(new Integer(SqlParserSymbols.BITNOT), "~");
    tokenIdMap.put(new Integer(SqlParserSymbols.LPAREN), "(");
    tokenIdMap.put(new Integer(SqlParserSymbols.RPAREN), ")");
    tokenIdMap.put(new Integer(SqlParserSymbols.LBRACKET), "[");
    tokenIdMap.put(new Integer(SqlParserSymbols.RBRACKET), "]");
    tokenIdMap.put(new Integer(SqlParserSymbols.SEMICOLON), ";");
    tokenIdMap.put(new Integer(SqlParserSymbols.FLOATINGPOINT_LITERAL),
        "FLOATING POINT LITERAL");
    tokenIdMap.put(new Integer(SqlParserSymbols.INTEGER_LITERAL), "INTEGER LITERAL");
    tokenIdMap.put(new Integer(SqlParserSymbols.LARGE_INTEGER_LITERAL), "INTEGER LITERAL");
    tokenIdMap.put(new Integer(SqlParserSymbols.DECIMAL_LITERAL), "DECIMAL LITERAL");
    tokenIdMap.put(new Integer(SqlParserSymbols.NOT), "!");
    tokenIdMap.put(new Integer(SqlParserSymbols.LESSTHAN), "<");
    tokenIdMap.put(new Integer(SqlParserSymbols.GREATERTHAN), ">");
    tokenIdMap.put(new Integer(SqlParserSymbols.UNMATCHED_STRING_LITERAL),
        "UNMATCHED STRING LITERAL");
    tokenIdMap.put(new Integer(SqlParserSymbols.MOD), "%");
    tokenIdMap.put(new Integer(SqlParserSymbols.ADD), "+");
    tokenIdMap.put(new Integer(SqlParserSymbols.DIVIDE), "/");
    tokenIdMap.put(new Integer(SqlParserSymbols.EQUAL), "=");
    tokenIdMap.put(new Integer(SqlParserSymbols.STAR), "*");
    tokenIdMap.put(new Integer(SqlParserSymbols.AT), "@");
    tokenIdMap.put(new Integer(SqlParserSymbols.BITOR), "|");
    tokenIdMap.put(new Integer(SqlParserSymbols.DOTDOTDOT), "...");
    tokenIdMap.put(new Integer(SqlParserSymbols.DOT), ".");
    tokenIdMap.put(new Integer(SqlParserSymbols.STRING_LITERAL), "STRING LITERAL");
    tokenIdMap.put(new Integer(SqlParserSymbols.EOF), "EOF");
    tokenIdMap.put(new Integer(SqlParserSymbols.SUBTRACT), "-");
    tokenIdMap.put(new Integer(SqlParserSymbols.BITAND), "&");
    tokenIdMap.put(new Integer(SqlParserSymbols.error), "ERROR");
    tokenIdMap.put(new Integer(SqlParserSymbols.BITXOR), "^");
    tokenIdMap.put(new Integer(SqlParserSymbols.NUMERIC_OVERFLOW), "NUMERIC OVERFLOW");
  }

  public static boolean isKeyword(Integer tokenId) {
    String token = tokenIdMap.get(tokenId);
    return keywordMap.containsKey(token);
    /* return keywordMap.containsKey(token.toLowerCase()); */
  }

  public static boolean isKeyword(String str) {
	    return keywordMap.containsKey(str.toLowerCase());
  }

  private Symbol newToken(int id, Object value) {
    return new Symbol(id, yyline+1, yycolumn+1, value);
  }

  private static String escapeBackSlash(String str) {
      StringWriter writer = new StringWriter();
      int strLen = str.length();
      for (int i = 0; i < strLen; ++i) {
          char c = str.charAt(i);
          if (c == '\\' && (i + 1) < strLen) {
              switch (str.charAt(i + 1)) {
              case 'n':
                  writer.append('\n');
                  break;
              case 't':
                  writer.append('\t');
                  break;
              case 'r':
                  writer.append('\r');
                  break;
              case 'b':
                  writer.append('\b');
                  break;
              case '0':
                  writer.append('\0'); // Ascii null
                  break;
              case 'Z': // ^Z must be escaped on Win32
                  writer.append('\032');
                  break;
              case '_':
              case '%':
                  writer.append('\\'); // remember prefix for wildcard
                  /* Fall through */
              default:
                  writer.append(str.charAt(i + 1));
                  break;
              }
              i++;
          } else {
              writer.append(c);
          }
      }

      return writer.toString();
  }
%}
%init{
    this.sql_mode = sql_mode;
%init}

LineTerminator = \r|\n|\r\n
NonTerminator = [^\r\n]
Whitespace = {LineTerminator} | [ \t\f]

IdentifierOrKwContents = [:digit:]*[:jletter:][:jletterdigit:]* | "&&" | "||"

IdentifierOrKw = \`{IdentifierOrKwContents}\` | {IdentifierOrKwContents}
IntegerLiteral = [:digit:][:digit:]*
QuotedIdentifier = \`(\`\`|[^\`])*\`
SingleQuoteStringLiteral = \'(\\.|[^\\\']|\'\')*\'
DoubleQuoteStringLiteral = \"(\\.|[^\\\"]|\"\")*\"

FLit1 = [0-9]+ \. [0-9]*
FLit2 = \. [0-9]+
FLit3 = [0-9]+
Exponent = [eE] [+-]? [0-9]+
DoubleLiteral = ({FLit1}|{FLit2}|{FLit3}) {Exponent}?

EolHintBegin = "--" " "* "+"
CommentedHintBegin = "/*" " "* "+"
CommentedTableTypeHintBegin = "/*" " "* "-"
CommentedTableTypeHintEnd =  "-" " "* "*/"
CommentedHintEnd = "*/"

// Both types of plan hints must appear within a single line.
HintContent = " "* "+" [^\r\n]*
HintContent2 = " "* "-" [^\r\n]*

Comment = {TraditionalComment} | {EndOfLineComment}

// Match anything that has a comment end (*/) in it.
ContainsCommentEnd = [^]* "*/" [^]*
// Match anything that has a line terminator in it.
ContainsLineTerminator = [^]* {LineTerminator} [^]*

// A traditional comment is anything that starts and ends like a comment and has neither a
// plan hint inside nor a CommentEnd (*/).
TraditionalComment = "/*" !({HintContent}|{HintContent2}|{ContainsCommentEnd}) "*/"
// Similar for a end-of-line comment.
EndOfLineComment = "--" !({HintContent}|{HintContent2}|{ContainsLineTerminator}) {LineTerminator}?

// This additional state is needed because newlines signal the end of a end-of-line hint
// if one has been started earlier. Hence we need to discern between newlines within and
// outside of end-of-line hints.
%state EOLHINT

%%

"..." { return newToken(SqlParserSymbols.DOTDOTDOT, null); }

// single-character tokens
"," { return newToken(SqlParserSymbols.COMMA, null); }
"." { return newToken(SqlParserSymbols.DOT, null); }
"*" { return newToken(SqlParserSymbols.STAR, null); }
"@" { return newToken(SqlParserSymbols.AT, null); }
"(" { return newToken(SqlParserSymbols.LPAREN, null); }
")" { return newToken(SqlParserSymbols.RPAREN, null); }
";" { return newToken(SqlParserSymbols.SEMICOLON, null); }
"[" { return newToken(SqlParserSymbols.LBRACKET, null); }
"]" { return newToken(SqlParserSymbols.RBRACKET, null); }
"/" { return newToken(SqlParserSymbols.DIVIDE, null); }
"%" { return newToken(SqlParserSymbols.MOD, null); }
"+" { return newToken(SqlParserSymbols.ADD, null); }
"-" { return newToken(SqlParserSymbols.SUBTRACT, null); }
"&" { return newToken(SqlParserSymbols.BITAND, null); }
"|" { return newToken(SqlParserSymbols.BITOR, null); }
"^" { return newToken(SqlParserSymbols.BITXOR, null); }
"~" { return newToken(SqlParserSymbols.BITNOT, null); }
"=" { return newToken(SqlParserSymbols.EQUAL, null); }
":=" { return newToken(SqlParserSymbols.SET_VAR, null); }
"!" { return newToken(SqlParserSymbols.NOT, null); }
"<" { return newToken(SqlParserSymbols.LESSTHAN, null); }
">" { return newToken(SqlParserSymbols.GREATERTHAN, null); }
"\"" { return newToken(SqlParserSymbols.UNMATCHED_STRING_LITERAL, null); }
"'" { return newToken(SqlParserSymbols.UNMATCHED_STRING_LITERAL, null); }
"`" { return newToken(SqlParserSymbols.UNMATCHED_STRING_LITERAL, null); }

{QuotedIdentifier} {
    // Remove the quotes
    String trimmedIdent = yytext().substring(1, yytext().length() - 1);
    return newToken(SqlParserSymbols.IDENT, SqlUtils.escapeUnquote(trimmedIdent));
}

{IdentifierOrKw} {
  String text = yytext();
  Integer kw_id = keywordMap.get(text.toLowerCase());
  /* Integer kw_id = keywordMap.get(text); */
  if (kw_id != null) {
    // if MODE_PIPES_AS_CONCAT is not active, treat '||' symbol as same as 'or' symbol
    if ((kw_id == SqlParserSymbols.KW_PIPE) &&
      ((this.sql_mode & SqlModeHelper.MODE_PIPES_AS_CONCAT) == 0)) {
      return newToken(SqlParserSymbols.KW_OR, text);
    }
    return newToken(kw_id.intValue(), text);
  } else {
    return newToken(SqlParserSymbols.IDENT, text);
  }
}

{SingleQuoteStringLiteral} {
  return newToken(SqlParserSymbols.STRING_LITERAL,
      escapeBackSlash(yytext().substring(1, yytext().length()-1)).replaceAll("''", "'"));
}

{DoubleQuoteStringLiteral} {
  return newToken(SqlParserSymbols.STRING_LITERAL,
      escapeBackSlash(yytext().substring(1, yytext().length()-1)).replaceAll("\"\"", "\""));
}

{IntegerLiteral} {
    BigInteger val = null;
    try {
        val = new BigInteger(yytext());
    } catch (NumberFormatException e) {
        return newToken(SqlParserSymbols.NUMERIC_OVERFLOW, yytext());
    }

    // Note: val is positive, because we do not recognize minus charactor in 'IntegerLiteral'
    // -2^63 will be recognize as largeint(__int128)
    if (val.compareTo(LONG_MAX) <= 0) {
        return newToken(SqlParserSymbols.INTEGER_LITERAL, val.longValue());
    }
    if (val.compareTo(LARGEINT_MAX_ABS) <= 0) {
        return newToken(SqlParserSymbols.LARGE_INTEGER_LITERAL, val.toString());
    }
    return newToken(SqlParserSymbols.NUMERIC_OVERFLOW, yytext());
}

{DoubleLiteral} {
  BigDecimal decimal_val;
  try {
    decimal_val = new BigDecimal(yytext());
  } catch (NumberFormatException e) {
    return newToken(SqlParserSymbols.NUMERIC_OVERFLOW, yytext());
  }

  return newToken(SqlParserSymbols.DECIMAL_LITERAL, decimal_val);
}

{Comment} { /* ignore */ }
{Whitespace} { /* ignore */ }
