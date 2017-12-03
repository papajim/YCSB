package com.yahoo.ycsb.db.flavors;

import com.yahoo.ycsb.db.JdbcDBClient;
import com.yahoo.ycsb.db.StatementType;

/**
 * An Oracle JSON flavor for schemaless data.
 */
public class OracleDBFlavor extends DBFlavor {
  public OracleDBFlavor() {
    super(DBName.ORACLE);
  }
  public OracleDBFlavor(DBName dbName) {
    super(dbName);
  }

  public static final String YCSB_DOC_COLUMN = "YCSB_DOC";

  @Override
  public String createInsertStatement(StatementType insertType, String key) {
    StringBuilder insert = new StringBuilder("INSERT INTO ");
    insert.append(insertType.getTableName());
    insert.append(" (" + JdbcDBClient.PRIMARY_KEY + "," + YCSB_DOC_COLUMN + ")");
    insert.append(" VALUES(?, ?)");
    return insert.toString();
  }

  @Override
  public String createReadStatement(StatementType readType, String key) {
    StringBuilder read = new StringBuilder("SELECT json_query("+ YCSB_DOC_COLUMN +", '$') as " + YCSB_DOC_COLUMN);
    //StringBuilder read = new StringBuilder("SELECT *");
    read.append(" FROM ");
    read.append(readType.getTableName());
    read.append(" WHERE ");
    read.append("json_value("+ YCSB_DOC_COLUMN + ", '$." + JdbcDBClient.PRIMARY_KEY + "')");
    //read.append(JdbcDBClient.PRIMARY_KEY);
    read.append(" = ");
    read.append("?");
    return read.toString();
  }

  @Override
  public String createDeleteStatement(StatementType deleteType, String key) {
    StringBuilder delete = new StringBuilder("DELETE FROM ");
    delete.append(deleteType.getTableName());
    delete.append(" WHERE ");
    delete.append("json_value("+ YCSB_DOC_COLUMN + ", '$." + JdbcDBClient.PRIMARY_KEY + "')");
    //delete.append(JdbcDBClient.PRIMARY_KEY);
    delete.append(" = ?");
    return delete.toString();
  }

  @Override
  public String createUpdateStatement(StatementType updateType, String key) {
    StringBuilder update = new StringBuilder("UPDATE ");
    update.append(updateType.getTableName());
    update.append(" SET ");
    update.append(YCSB_DOC_COLUMN);
    update.append(" = ?");
    update.append(" WHERE ");
    update.append("json_value("+ YCSB_DOC_COLUMN + ", '$." + JdbcDBClient.PRIMARY_KEY + "')");
    //update.append(JdbcDBClient.PRIMARY_KEY);
    update.append(" = ?");
    return update.toString();
  }

  @Override
  public String createScanStatement(StatementType scanType, String key) {
    StringBuilder select = new StringBuilder("SELECT json_query("+ YCSB_DOC_COLUMN +", '$') as " + YCSB_DOC_COLUMN);
    //StringBuilder select = new StringBuilder("SELECT *");
    select.append(" FROM ");
    select.append(scanType.getTableName());
    select.append(" WHERE ");
    select.append("json_value("+ YCSB_DOC_COLUMN + ", '$." + JdbcDBClient.PRIMARY_KEY + "')");
    //select.append(JdbcDBClient.PRIMARY_KEY);
    select.append(" >= ?");
    select.append(" ORDER BY ");
    select.append("json_value("+ YCSB_DOC_COLUMN + ", '$." + JdbcDBClient.PRIMARY_KEY + "')");
    //select.append(JdbcDBClient.PRIMARY_KEY);
    select.append(" LIMIT ?");
    return select.toString();
  }
}

