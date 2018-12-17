/*
 * Copyright (c) 2011-2014 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *     The Eclipse Public License is available at
 *     http://www.eclipse.org/legal/epl-v10.html
 *
 *     The Apache License v2.0 is available at
 *     http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package io.vertx.ext.jdbc.impl.actions;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.TaskQueue;
import io.vertx.core.spi.tracing.Tracer;
import io.vertx.ext.sql.SQLOptions;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author <a href="mailto:nscavell@redhat.com">Nick Scavelli</a>
 * @author <a href="mailto:plopes@redhat.com">Paulo Lopes</a>
 */
public abstract class AbstractJDBCAction<T> {

  protected final Vertx vertx;
  protected final SQLOptions options;
  protected final ContextInternal ctx;
  protected final JDBCStatementHelper helper;

  protected AbstractJDBCAction(SQLOptions options, ContextInternal ctx) {
    this(null, options, ctx);
  }

  protected AbstractJDBCAction(JDBCStatementHelper helper, SQLOptions options, ContextInternal ctx) {
    this.vertx = ctx.owner();
    this.options = options;
    this.ctx = ctx;
    this.helper = helper;
  }

  private void handle(Connection conn, Future<T> future) {
    try {
      // apply connection options
      applyConnectionOptions(conn);
      // execute
      T result = execute(conn);
      future.complete(result);
    } catch (SQLException e) {
      future.fail(e);
    }
  }

  public void execute(Connection conn, TaskQueue statementsQueue, Handler<AsyncResult<T>> resultHandler) {
    Tracer tracer = ctx.owner().tracer();
    Object trace;
    if (tracer != null) {
      trace = tracer.sendRequest(ctx.localContextData(), this);
    } else {
      trace = null;
    }
    ctx.<T>executeBlocking(future -> handle(conn, future), statementsQueue, ar -> {
      if (tracer != null) {
        tracer.receiveResponse(ctx.localContextData(), ar.result(), trace, ar.cause());
      }
      resultHandler.handle(ar);
    });
  }

  public abstract T execute(Connection conn) throws SQLException;

  protected abstract String name();

  public String statement() {
    return name();
  }

  void applyStatementOptions(Statement statement) throws SQLException {
    if (options != null) {
      if (options.getQueryTimeout() > 0) {
        statement.setQueryTimeout(options.getQueryTimeout());
      }
      if (options.getFetchDirection() != null) {
        statement.setFetchDirection(options.getFetchDirection().getType());
      }
      if (options.getFetchSize() > 0) {
        statement.setFetchSize(options.getFetchSize());
      }
    }
  }

  private void applyConnectionOptions(Connection conn) throws SQLException {
    if (options != null) {
      if (options.isReadOnly()) {
        conn.setReadOnly(true);
      }
      if (options.getCatalog() != null) {
        conn.setCatalog(options.getCatalog());
      }
      if (options.getSchema() != null) {
        conn.setSchema(options.getSchema());
      }
    }
  }

}
