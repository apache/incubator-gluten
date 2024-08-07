/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.parser

import org.apache.gluten.sql.parser.{GlutenCacheFileSqlBaseBaseListener, GlutenCacheFileSqlBaseBaseVisitor, GlutenCacheFileSqlBaseLexer, GlutenCacheFileSqlBaseParser}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.parser.{ParseErrorListener, ParseException, ParserInterface}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.execution.commands.GlutenCacheFilesCommand
import org.apache.spark.sql.internal.VariableSubstitution

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.ParseCancellationException
import org.antlr.v4.runtime.tree.TerminalNodeImpl

import java.util.Locale

import scala.collection.JavaConverters._

trait GlutenCacheFileSqlParserBase extends ParserInterface {
  protected val astBuilder = new GlutenCacheFileSqlAstBuilder
  protected val substitution = new VariableSubstitution

  protected def parse[T](command: String)(toResult: GlutenCacheFileSqlBaseParser => T): T = {
    val lexer = new GlutenCacheFileSqlBaseLexer(
      new UpperCaseCharStream(CharStreams.fromString(substitution.substitute(command))))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new GlutenCacheFileSqlBaseParser(tokenStream)

    parser.addParseListener(GlutenCacheFileSqlPostProcessor)
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)

    try {
      try {
        // first, try parsing with potentially faster SLL mode
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        toResult(parser)
      } catch {
        case e: ParseCancellationException =>
          // if we fail, parse with LL mode
          tokenStream.seek(0) // rewind input stream
          parser.reset()

          // Try Again.
          parser.getInterpreter.setPredictionMode(PredictionMode.LL)
          toResult(parser)
      }
    } catch {
      case e: ParseException if e.command.isDefined =>
        throw e
      case e: ParseException =>
        throw e.withCommand(command)
      case e: AnalysisException =>
        val position = Origin(e.line, e.startPosition)
        throw new ParseException(
          command = Option(command),
          message = e.message,
          start = position,
          stop = position,
          errorClass = Some("GLUTEN_CACHE_FILE_PARSING_ANALYSIS_ERROR"))
    }
  }
}

class GlutenCacheFileSqlAstBuilder extends GlutenCacheFileSqlBaseBaseVisitor[AnyRef] {
  import org.apache.spark.sql.catalyst.parser.ParserUtils._

  /** Convert a property list into a key-value map. */
  override def visitPropertyList(
      ctx: GlutenCacheFileSqlBaseParser.PropertyListContext): Map[String, String] =
    withOrigin(ctx) {
      val properties = ctx.property.asScala.map {
        property =>
          val key = visitPropertyKey(property.key)
          val value = visitPropertyValue(property.value)
          key -> value
      }
      // Check for duplicate property names.
      checkDuplicateKeys(properties.toSeq, ctx)
      properties.toMap
    }

  /**
   * A property key can either be String or a collection of dot separated elements. This function
   * extracts the property key based on whether its a string literal or a property identifier.
   */
  override def visitPropertyKey(key: GlutenCacheFileSqlBaseParser.PropertyKeyContext): String = {
    if (key.stringLit() != null) {
      string(visitStringLit(key.stringLit()))
    } else {
      key.getText
    }
  }

  /**
   * A property value can be String, Integer, Boolean or Decimal. This function extracts the
   * property value based on whether its a string, integer, boolean or decimal literal.
   */
  override def visitPropertyValue(
      value: GlutenCacheFileSqlBaseParser.PropertyValueContext): String = {
    if (value == null) {
      null
    } else if (value.identifier != null) {
      value.identifier.getText
    } else if (value.value != null) {
      string(visitStringLit(value.value))
    } else if (value.booleanValue != null) {
      value.getText.toLowerCase(Locale.ROOT)
    } else {
      value.getText
    }
  }

  def visitPropertyKeyValues(
      ctx: GlutenCacheFileSqlBaseParser.PropertyListContext): Map[String, String] = {
    val props = visitPropertyList(ctx)
    val badKeys = props.collect { case (key, null) => key }
    if (badKeys.nonEmpty) {
      operationNotAllowed(
        s"Values must be specified for key(s): ${badKeys.mkString("[", ",", "]")}",
        ctx)
    }
    props
  }

  override def visitStringLit(ctx: GlutenCacheFileSqlBaseParser.StringLitContext): Token = {
    if (ctx != null) {
      if (ctx.STRING != null) {
        ctx.STRING.getSymbol
      } else {
        ctx.DOUBLEQUOTED_STRING.getSymbol
      }
    } else {
      null
    }
  }

  override def visitSingleStatement(
      ctx: GlutenCacheFileSqlBaseParser.SingleStatementContext): AnyRef = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }

  override def visitCacheFiles(ctx: GlutenCacheFileSqlBaseParser.CacheFilesContext): AnyRef =
    withOrigin(ctx) {
      val asynExecute = ctx.ASYNC != null
      val selectedColuman = visitSelectedColumnNames(ctx.selectedColumns)
      val propertyOverrides = Option(ctx.cacheProps)
        .map(visitPropertyKeyValues)
        .getOrElse(Map.empty[String, String])
      val path = ctx.path.getText

      GlutenCacheFilesCommand(
        asynExecute,
        selectedColuman,
        path.substring(1, path.length - 1),
        propertyOverrides
      )
    }

  override def visitPassThrough(ctx: GlutenCacheFileSqlBaseParser.PassThroughContext): AnyRef =
    null

  override def visitSelectedColumnNames(
      ctx: GlutenCacheFileSqlBaseParser.SelectedColumnNamesContext): Option[Seq[String]] =
    withOrigin(ctx) {
      if (ctx != null) {
        if (ctx.ASTERISK != null) {
          // It means select all columns
          None
        } else if (ctx.identifier != null && !(ctx.identifier).isEmpty) {
          Some(ctx.identifier.asScala.map(_.getText).toSeq)
        } else {
          throw new ParseException(s"Illegal selected column.", ctx)
        }
      } else {
        throw new ParseException(s"Illegal selected column.", ctx)
      }
    }
}

case object GlutenCacheFileSqlPostProcessor extends GlutenCacheFileSqlBaseBaseListener {

  /** Remove the back ticks from an Identifier. */
  override def exitQuotedIdentifier(
      ctx: GlutenCacheFileSqlBaseParser.QuotedIdentifierContext): Unit = {
    replaceTokenByIdentifier(ctx, 1) {
      token =>
        // Remove the double back ticks in the string.
        token.setText(token.getText.replace("``", "`"))
        token
    }
  }

  /** Treat non-reserved keywords as Identifiers. */
  override def exitNonReserved(ctx: GlutenCacheFileSqlBaseParser.NonReservedContext): Unit = {
    replaceTokenByIdentifier(ctx, 0)(identity)
  }

  private def replaceTokenByIdentifier(ctx: ParserRuleContext, stripMargins: Int)(
      f: CommonToken => CommonToken = identity): Unit = {
    val parent = ctx.getParent
    parent.removeLastChild()
    val token = ctx.getChild(0).getPayload.asInstanceOf[Token]
    val newToken = new CommonToken(
      new org.antlr.v4.runtime.misc.Pair(token.getTokenSource, token.getInputStream),
      GlutenCacheFileSqlBaseParser.IDENTIFIER,
      token.getChannel,
      token.getStartIndex + stripMargins,
      token.getStopIndex - stripMargins
    )
    parent.addChild(new TerminalNodeImpl(f(newToken)))
  }
}
