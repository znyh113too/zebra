/**
 * Project: ${zebra-client.aid}
 * 
 * File Created at 2011-6-10 $Id$
 * 
 * Copyright 2010 dianping.com. All rights reserved.
 * 
 * This software is the confidential and proprietary information of Dianping
 * Company. ("Confidential Information"). You shall not disclose such
 * Confidential Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with dianping.com.
 */
package com.dianping.zebra.shard.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import com.dianping.zebra.shard.jdbc.parallel.SQLThreadPoolExecutor;
import com.dianping.zebra.shard.jdbc.parallel.StatementExecuteQueryCallable;
import com.dianping.zebra.shard.jdbc.parallel.StatementExecuteUpdateCallable;
import com.dianping.zebra.shard.jdbc.parallel.UpdateResult;
import com.dianping.zebra.shard.jdbc.unsupport.UnsupportedShardStatement;
import com.dianping.zebra.shard.router.RouterResult;
import com.dianping.zebra.shard.router.RouterResult.RouterTarget;
import com.dianping.zebra.shard.router.ShardRouter;
import com.dianping.zebra.util.JDBCUtils;
import com.dianping.zebra.util.SqlType;
import com.dianping.zebra.util.SqlUtils;

/**
 * @author Leo Liang
 * @author hao.zhu
 * 
 */
public class ShardStatement extends UnsupportedShardStatement implements Statement {

    private static final String SELECT_GENERATEDKEY_SQL_PATTERN = "@@identity";
    private static final String SELECT_LAST_INSERT_ID = "last_insert_id()";
    protected ShardConnection connection;
    protected boolean autoCommit = true;
	protected Set<ResultSet> attachedResultSets = new HashSet<ResultSet>();
	protected List<Statement> actualStatements = new ArrayList<Statement>();
	protected ResultSet results;
	protected int updateCount = -1;
	protected ResultSet generatedKey;
    private ShardRouter router;
    private boolean closed;
    private boolean readOnly;
    private int resultSetType = -1;
    private int resultSetConcurrency = -1;
    private int resultSetHoldability = -1;

	private boolean executeInternal(String sql, int autoGeneratedKeys, int[] columnIndexes, String[] columnNames)
			throws SQLException {
		SqlType sqlType = judgeSqlType(sql);
		if (sqlType == SqlType.SELECT) {
			executeQuery(sql);
			return true;
		} else if (sqlType == SqlType.INSERT || sqlType == SqlType.UPDATE || sqlType == SqlType.DELETE) {
			if (autoGeneratedKeys == -1 && columnIndexes == null && columnNames == null) {
				executeUpdate(sql);
			} else if (autoGeneratedKeys != -1) {
				executeUpdate(sql, autoGeneratedKeys);
			} else if (columnIndexes != null) {
				executeUpdate(sql, columnIndexes);
			} else if (columnNames != null) {
				executeUpdate(sql, columnNames);
			} else {
				executeUpdate(sql);
			}

			return false;
		} else {
			throw new SQLException("only select, insert, update, delete sql is supported");
		}
	}

	private int executeUpdateInternal(String sql, int autoGeneratedKeys, int[] columnIndexes, String[] columnNames)
			throws SQLException {
		checkClosed();

		RouterResult routerTarget = routingAndCheck(sql, null);

		int affectedRows = 0;
		if (isSingleTarget(routerTarget)) {
			// if has only one sql,then serial execute it
			for (RouterTarget targetedSql : routerTarget.getSqls()) {
				for (String executableSql : targetedSql.getSqls()) {
					Connection conn = connection.getRealConnection(targetedSql.getDatabaseName(), autoCommit);
					Statement stmt = createStatement(conn);
					actualStatements.add(stmt);

					if (autoGeneratedKeys == -1 && columnIndexes == null && columnNames == null) {
						affectedRows += stmt.executeUpdate(executableSql, Statement.RETURN_GENERATED_KEYS);
					} else if (autoGeneratedKeys != -1) {
						affectedRows += stmt.executeUpdate(executableSql, autoGeneratedKeys);
					} else if (columnIndexes != null) {
						affectedRows += stmt.executeUpdate(executableSql, columnIndexes);
					} else if (columnNames != null) {
						affectedRows += stmt.executeUpdate(executableSql, columnNames);
					} else {
						affectedRows += stmt.executeUpdate(executableSql, Statement.RETURN_GENERATED_KEYS);
					}
					
					if(autoGeneratedKeys != -1){
						this.generatedKey = stmt.getGeneratedKeys();
					}
				}
			}
		} else {
			// if has multiple sqls,then parallel execute them
			List<Callable<UpdateResult>> tasks = new ArrayList<Callable<UpdateResult>>();
			for (RouterTarget targetedSql : routerTarget.getSqls()) {
				for (String executableSql : targetedSql.getSqls()) {
					Connection conn = connection.getRealConnection(targetedSql.getDatabaseName(), autoCommit);
					Statement stmt = createStatement(conn);
					actualStatements.add(stmt);
					tasks.add(new StatementExecuteUpdateCallable(stmt, executableSql, autoGeneratedKeys, columnIndexes,
							columnNames));
				}
			}

			List<Future<UpdateResult>> futures = SQLThreadPoolExecutor.getInstance().invokeSQLs(tasks);
			for (Future<UpdateResult> f : futures) {
				try {
					UpdateResult updateResult = f.get();

					affectedRows += updateResult.getAffectedRows();
					if (updateResult.getGeneratedKey() != null) {
						this.generatedKey = updateResult.getGeneratedKey();
					}
				} catch (Exception e) {
					// normally can't be here
					throw new SQLException(e);
				}
			}
		}

		this.results = null;
		this.updateCount = affectedRows;

		return affectedRows;
	}

	protected ResultSet beforeQuery(String sql) throws SQLException {
		// 特殊处理 SELECT @@IDENTITY AS A
		// 这种SQL，因为这种SQL需要从同一个DPConnection会话中获得上次Insert语句的返回值
		ResultSet generatedKey = this.generatedKey;
		sql = sql.toLowerCase();
		if (generatedKey != null && sql != null
				&& (sql.indexOf(SELECT_GENERATEDKEY_SQL_PATTERN) >= 0 || sql.indexOf(SELECT_LAST_INSERT_ID) >= 0)) {
			List<ResultSet> rsList = new ArrayList<ResultSet>();
			generatedKey.beforeFirst();
			rsList.add(generatedKey);

			ShardResultSet rs = new ShardResultSet();
			rs.setStatement(this);
			rs.setResultSets(rsList);

			attachedResultSets.add(rs);

			this.results = rs;
			this.updateCount = -1;

			return this.results;
		}

		return null;
	}

	protected void checkClosed() throws SQLException {
		if (closed) {
			throw new SQLException("No operations allowed after statement closed.");
		}
	}

	@Override
	public void close() throws SQLException {
		if (closed) {
			return;
		}

		List<SQLException> exceptions = new ArrayList<SQLException>();

		try {
			for (ResultSet resultSet : attachedResultSets) {
				try {
					resultSet.close();
				} catch (SQLException e) {
					exceptions.add(e);
				}
			}

			for (Statement stmt : actualStatements) {
				try {
					stmt.close();
				} catch (SQLException e) {
					exceptions.add(e);
				}
			}
		} finally {
			closed = true;
			attachedResultSets.clear();
			actualStatements.clear();
			results = null;
		}

		JDBCUtils.throwSQLExceptionIfNeeded(exceptions);
	}

	private Statement createStatement(Connection connection) throws SQLException {
		Statement stmt;
		if (this.resultSetType != -1 && this.resultSetConcurrency != -1 && this.resultSetHoldability != -1) {
			stmt = connection.createStatement(this.resultSetType, this.resultSetConcurrency, this.resultSetHoldability);
		} else if (this.resultSetType != -1 && this.resultSetConcurrency != -1) {
			stmt = connection.createStatement(this.resultSetType, this.resultSetConcurrency);
		} else {
			stmt = connection.createStatement();
		}

		return stmt;
	}

	protected void executableCheck(RouterResult routerTarget) throws SQLException {
		if (routerTarget == null) {
			throw new SQLException("No router return value.");
		}
		// TODO 可以增加更多限制
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		return executeInternal(sql, -1, null, null);
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		return executeInternal(sql, autoGeneratedKeys, null, null);
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		return executeInternal(sql, -1, columnIndexes, null);
	}

	@Override
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		return executeInternal(sql, -1, null, columnNames);
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		checkClosed();

		ResultSet specRS = beforeQuery(sql);
		if (specRS != null) {
			this.results = specRS;
			this.updateCount = -1;
			attachedResultSets.add(specRS);
			return this.results;
		}

		RouterResult routerTarget = routingAndCheck(sql, null);

		ShardResultSet rs = new ShardResultSet();
		rs.setStatement(this);
		rs.setRouterTarget(routerTarget);

		attachedResultSets.add(rs);

		if (isSingleTarget(routerTarget)) {
			// if has only one sql,then serial execute it
			for (RouterTarget targetedSql : routerTarget.getSqls()) {
				for (String executableSql : targetedSql.getSqls()) {

                    // 从刚才解析出来的dbName获取真正的数据库连接对象
					Connection conn = connection.getRealConnection(targetedSql.getDatabaseName(), autoCommit);
					Statement stmt = createStatement(conn);
					actualStatements.add(stmt);

                    // 执行stmt,收集默认的结果,供后续合并数据
					rs.addResultSet(stmt.executeQuery(executableSql));
				}
			}
		} else {
            // 使用线程池并发处理sql执行
			List<Callable<ResultSet>> callables = new ArrayList<Callable<ResultSet>>();
			for (RouterTarget targetedSql : routerTarget.getSqls()) {
				for (String executableSql : targetedSql.getSqls()) {
					Connection conn = connection.getRealConnection(targetedSql.getDatabaseName(), autoCommit);
					Statement stmt = createStatement(conn);
					actualStatements.add(stmt);

					callables.add(new StatementExecuteQueryCallable(stmt, executableSql));
				}
			}

			List<Future<ResultSet>> futures = SQLThreadPoolExecutor.getInstance().invokeSQLs(callables);
			for (Future<ResultSet> f : futures) {
				try {
					rs.addResultSet(f.get());
				} catch (Exception e) {
					// normally can't be here!
					throw new SQLException(e);
				}
			}
		}

		this.results = rs;
		this.updateCount = -1;

        // 将结果合并
		rs.init();

		// JDBCUtils.throwSQLExceptionIfNeeded(exceptions);

		return this.results;

	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		return executeUpdateInternal(sql, -1, null, null);
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		return executeUpdateInternal(sql, autoGeneratedKeys, null, null);
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		return executeUpdateInternal(sql, -1, columnIndexes, null);
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		return executeUpdateInternal(sql, -1, null, columnNames);
	}

	public Set<ResultSet> getAttachedResultSets() {
		return attachedResultSets;
	}

    public void setAttachedResultSets(Set<ResultSet> attachedResultSets) {
        this.attachedResultSets = attachedResultSets;
    }

	@Override
	public Connection getConnection() throws SQLException {
		return connection;
	}

    public void setConnection(ShardConnection dpConnection) {
        this.connection = dpConnection;
    }

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		return this.generatedKey;
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		return false;
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		return results;
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		return resultSetConcurrency;
	}

    public void setResultSetConcurrency(int resultSetConcurrency) {
        this.resultSetConcurrency = resultSetConcurrency;
    }

	@Override
	public int getResultSetHoldability() throws SQLException {
		return resultSetHoldability;
	}

    public void setResultSetHoldability(int resultSetHoldability) {
        this.resultSetHoldability = resultSetHoldability;
    }

	@Override
	public int getResultSetType() throws SQLException {
		return resultSetType;
	}

    public void setResultSetType(int resultSetType) {
        this.resultSetType = resultSetType;
    }

	public ShardRouter getRouter() {
		return router;
	}

    public void setRouter(ShardRouter router) {
        this.router = router;
    }

	@Override
	public int getUpdateCount() throws SQLException {
		return updateCount;
	}

	public boolean isAutoCommit() {
		return autoCommit;
	}

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

	@Override
	public boolean isClosed() throws SQLException {
		return closed;
	}

	public boolean isReadOnly() {
		return readOnly;
	}

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

	protected SqlType judgeSqlType(String sql) throws SQLException {
		SqlType sqlType = SqlUtils.getSqlType(sql);

		if (sqlType != SqlType.SELECT && sqlType != SqlType.INSERT && sqlType != SqlType.UPDATE
				&& sqlType != SqlType.DELETE) {
			throw new SQLException("Only select, insert, update, delete sql is supported.");
		}

		return sqlType;
	}

	protected RouterResult routingAndCheck(String sql, List<Object> params) throws SQLException {
		RouterResult routerTarget = null;

		try {
			routerTarget = router.router(sql, params);
			executableCheck(routerTarget);
		} catch (Exception e) {
			throw new SQLException(e);
		}

		return routerTarget;
	}

	protected boolean isSingleTarget(RouterResult rr) {
		if (rr.getSqls().size() > 1) {
			return false;
		}
		RouterTarget rt = rr.getSqls().get(0);

		if (rt.getSqls().size() > 1) {
			return false;
		}

		return true;
	}
}
