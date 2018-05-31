package com.dianping.zebra.shard.parser;

import java.util.Collections;
import java.util.Map;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlLexer;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.Lexer.CommentHandler;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.parser.Token;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;
import com.dianping.zebra.shard.exception.ShardParseException;
import com.dianping.zebra.shard.parser.visitor.MySQLDeleteASTVisitor;
import com.dianping.zebra.shard.parser.visitor.MySQLInsertASTVisitor;
import com.dianping.zebra.shard.parser.visitor.MySQLSelectASTVisitor;
import com.dianping.zebra.shard.parser.visitor.MySQLUpdateASTVisitor;
import com.dianping.zebra.shard.util.LRUCache;
import com.dianping.zebra.util.SqlType;

public class SQLParser {

	private final static Map<String, SQLParsedResult> parsedSqlCache = Collections
			.synchronizedMap(new LRUCache<String, SQLParsedResult>(1000));

	public static SQLParsedResult parse(String sql) throws ShardParseException {
        // 做一层LRU缓存
		if (!parsedSqlCache.containsKey(sql)) {

            // 引入druid的SQL Parse模块,这里只使用了MySql方言,也就是不支持其他数据库分库分表咯
			MySqlLexer lexer = new MySqlLexer(sql);
			HintCommentHandler commentHandler = new HintCommentHandler();
			lexer.setCommentHandler(commentHandler);
			lexer.nextToken();

            // 创建Mysql Parser对象
			SQLStatementParser parser = new MySqlStatementParser(lexer);
            // 生成AST对象
			SQLStatement stmt = parser.parseStatement();
			SQLParsedResult result = null;

            // 根据不同的stmt对象使用不同的访问者访问,基本上都获取表名存到SQLParsedResult对象中供后续逻辑表名替换物理表名用
			if (stmt instanceof SQLSelectStatement) {
				result = new SQLParsedResult(SqlType.SELECT, stmt);
                // 查询sql的访问者获取了查询参数,groupBy,limit等参数组装到SQLParsedResult对象中用于后续创建新sql用
				SQLASTVisitor visitor = new MySQLSelectASTVisitor(result);
				stmt.accept(visitor);
			} else if (stmt instanceof SQLInsertStatement) {
				result = new SQLParsedResult(SqlType.INSERT, stmt);
				SQLASTVisitor visitor = new MySQLInsertASTVisitor(result);
				stmt.accept(visitor);
			} else if (stmt instanceof SQLUpdateStatement) {
				result = new SQLParsedResult(SqlType.UPDATE, stmt);
				SQLASTVisitor visitor = new MySQLUpdateASTVisitor(result);
				stmt.accept(visitor);
			} else if (stmt instanceof SQLDeleteStatement) {
				result = new SQLParsedResult(SqlType.DELETE, stmt);
				SQLASTVisitor visitor = new MySQLDeleteASTVisitor(result);
				stmt.accept(visitor);
			} else {
				throw new ShardParseException("UnSupported sql type in sharding jdbc.");
			}

            // 收集关注的注释
			SQLHint sqlhint = SQLHint.parseHint(commentHandler.getHintComment());
			result.getRouterContext().setSqlhint(sqlhint);

			parsedSqlCache.put(sql, result);
		}

		return parsedSqlCache.get(sql);
	}

	public static class HintCommentHandler implements CommentHandler {

		private String hintComment = null;

		@Override
		public boolean handle(Token lastToken, String comment) {
			if (lastToken == null && comment.contains("zebra")) {
				hintComment = comment;
			}
			return false;
		}

		public String getHintComment() {
			return hintComment;
		}
	}
}
