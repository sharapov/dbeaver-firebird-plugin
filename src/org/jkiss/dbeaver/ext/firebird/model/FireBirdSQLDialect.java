/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2019 Serge Rider (serge@jkiss.org)
 *
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
package org.jkiss.dbeaver.ext.firebird.model;

import java.util.Arrays;
import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.ext.generic.model.GenericSQLDialect;
import org.jkiss.dbeaver.model.DBPEvaluationContext;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCDatabaseMetaData;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCDataSource;
import org.jkiss.dbeaver.model.struct.rdb.DBSProcedure;

public class FireBirdSQLDialect extends GenericSQLDialect {

    public static final String[] FB_BLOCK_HEADERS = new String[]{
        "EXECUTE BLOCK",
        "DECLARE", //"IS",
    };

    public static final String[][] FB_BEGIN_END_BLOCK = new String[][]{
        {"BEGIN",
            "END"},};

    private static final String[] DDL_KEYWORDS = new String[]{
        "CREATE",
        "ALTER",
        "DROP",
        "EXECUTE",
        "EXECUTE",
        "DATABASE",
        "SHADOW",
        "DOMAIN",
        "TABLE",
        "INDEX",
        "VIEW",
        "RECREATE",
        "TRIGGER",
        "PROCEDURE",
        "FUNCTION",
        "PACKAGE",
        "BODY",
        "FILTER",
        "SEQUENCE",
        "EXCEPTION",
        "COLLATION",
        "ROLE",
        "COMMENTS"
    };

    public FireBirdSQLDialect() {
        super("FireBird");
    }

    @NotNull
    @Override
    public String[] getDDLKeywords() {
        return DDL_KEYWORDS;
    }

    @Override
    public String[] getBlockHeaderStrings() {
        return FB_BLOCK_HEADERS;
    }

    @Override
    public String[][] getBlockBoundStrings() {
        return FB_BEGIN_END_BLOCK;
    }

    @Override
    public void initDriverSettings(JDBCDataSource dataSource, JDBCDatabaseMetaData metaData) {
        super.initDriverSettings(dataSource, metaData);
        addFunctions(
                Arrays.asList(
                        "CURRENT_CONNECTION", "CURRENT_DATE",
                        "CURRENT_TIME",
                        "CURRENT_TIMESTAMP",
                        "DATEADD",
                        "DATEDIFF",
                        "DOW",
                        "EXTRACT",
                        //String functions
                        "ASCII_CHAR",
                        "ASCII_VAL",
                        "BIT_LENGTH",
                        "CHAR_LENGTH",
                        "CHAR_TO_UUID",
                        "CURRENT_ROLE",
                        "CURRENT_USER",
                        "GEN_UUID",
                        "LPAD",
                        "LEFT",
                        "LIST",
                        "LOWER",
                        "OCTET_LENGTH",
                        "OVERLAY",
                        "POSITION",
                        "RDB$GET_CONTEXT",
                        "RDB$SET_CONTEXT",
                        "REPLACE",
                        "REVERSE",
                        "RIGHT",
                        "RPAD",
                        "SUBSTRING",
                        "TRIM",
                        "UPPER",
                        "UUID_TO_CHAR",
                        //Math functions
                        "ABS",
                        "ACOS",
                        "ASIN",
                        "ATAN",
                        "ATAN2",
                        "AVG",
                        "BIN_AND",
                        "BIN_OR",
                        "BIN_SHL",
                        "BIN_SHR",
                        "BIN_XOR",
                        "CEILING",
                        "CHECK_POINT_LEN",
                        "COS",
                        "COSH",
                        "COT",
                        "COUNT",
                        "DIV",
                        "EXP",
                        "FLOOR",
                        "GEN_ID",
                        "HASH",
                        "LN",
                        "LOG",
                        "LOG10",
                        "MAX",
                        "MAXVALUE",
                        "MIN",
                        "MINVALUE",
                        "MOD",
                        "PI",
                        "POWER",
                        "RAND",
                        "ROUND",
                        "SIGN",
                        "SIN",
                        "SINH",
                        "SQRT",
                        "SUM",
                        "TAN",
                        "TANH",
                        "TRUNC",
                        //Other functions
                        "CAST",
                        "IIF",
                        "CASE",
                        "DECODE",
                        "COALESCE",
                        "NULLIF"
                ));
    }

    @Override
    public boolean supportsAliasInSelect() {
        return true;
    }

    @Override
    public boolean validIdentifierPart(char c, boolean quoted) {
        return super.validIdentifierPart(c, quoted) || c == '$';
    }

    @Override
    protected String getStoredProcedureCallInitialClause(DBSProcedure proc) {
        return "select * from " + proc.getFullyQualifiedName(DBPEvaluationContext.DML);
    }

    @Override
    public String getDualTableName() {
        return "RDB$DATABASE";
    }
}
