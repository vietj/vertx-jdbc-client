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

import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.TaskQueue;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLOptions;
import io.vertx.ext.sql.SQLRowStream;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author <a href="mailto:plopes@redhat.com">Paulo Lopes</a>
 */
public class StreamQuery extends AbstractJDBCAction<SQLRowStream> {

  private static final Pattern CALL = Pattern.compile("=?\\s*call\\s+", Pattern.CASE_INSENSITIVE + Pattern.MULTILINE);
  private static final int DEFAULT_ROW_STREAM_FETCH_SIZE = 128;

  private final String sql;
  private final JsonArray in;
  private final TaskQueue statementsQueue;

  public StreamQuery(Vertx vertx, JDBCStatementHelper helper, SQLOptions options, ContextInternal ctx, TaskQueue statementsQueue, String sql, JsonArray in) {
    super(vertx, helper, options, ctx);
    this.sql = sql;
    this.in = in;
    this.statementsQueue = statementsQueue;
    // set the default fetch size
    if (options.getFetchSize() <= 0) {
      options.setFetchSize(DEFAULT_ROW_STREAM_FETCH_SIZE);
    }
  }

  @Override
  public SQLRowStream execute(Connection conn) throws SQLException {
    PreparedStatement st = null;

    try {
      // identify if this is a callable statement or a prepared statement
      Matcher matcher = CALL.matcher(sql);
      if (matcher.find()) {
        st = conn.prepareCall(sql);
      } else {
        st = conn.prepareStatement(sql);
      }
      // apply statement options
      applyStatementOptions(st);

      helper.fillStatement(st, in);
      ResultSet rs = null;

      try {
        if (st.execute()) {
          rs = st.getResultSet();
        }

        final int fetchSize;

        if (options != null && options.getFetchSize() > 0) {
          fetchSize = options.getFetchSize();
        } else {
          fetchSize = DEFAULT_ROW_STREAM_FETCH_SIZE;
        }

        return new JDBCSQLRowStream(ctx, this.statementsQueue, st, rs, fetchSize);
      } catch (SQLException e) {
        if (rs != null) {
          rs.close();
        }
        throw e;
      }
    } catch (SQLException e) {
      if (st != null) {
        st.close();
      }
      throw e;
    }
  }

  @Override
  protected String name() {
    return "stream";
  }
}
