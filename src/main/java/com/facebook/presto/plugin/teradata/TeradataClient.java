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

package com.facebook.presto.plugin.teradata;

import com.facebook.presto.plugin.jdbc.*;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.teradata.jdbc.TeraDriver;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.Iterables.getOnlyElement;

/**
 * Implementation of TeradataClient. It describes table, schemas and columns behaviours.
 * It allows to change the QueryBuilder to a custom one as well.
 */
public class TeradataClient extends BaseJdbcClient {
    private static final Logger log = Logger.getLogger(TeradataClient.class);

    @Inject
    public TeradataClient(JdbcConnectorId connectorId, BaseJdbcConfig config, TeradataConfig teradataConfig) throws SQLException {
        /*
            We use an empty string as identifierQuote parameter, to avoid using quotes when creating queries
            The following properties are already set to BaseJdbcClient, via properties injection:
            - connectionProperties.setProperty("user", teradataConfig.getUser());
            - connectionProperties.setProperty("url", teradataConfig.getUrl());
            - connectionProperties.setProperty("password", teradataConfig.getPassword());
         */
        super(connectorId, config, "", new TeraDriver());
    }

    @Override
    public Set<String> getSchemaNames() {
        Connection connection = null;
        try{
            connection = driver.connect(connectionUrl, connectionProperties);
            ResultSet resultSet = connection.getMetaData().getSchemas();
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString(1).toLowerCase();
                log.info("Schemas list: " + schemaName);
                schemaNames.add(schemaName);
            }
            return schemaNames.build();
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }finally {
            if(connection!=null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    log.warn("Failed closing connection", e);
                }
            }
        }
    }

    protected ResultSet getTables(Connection connection, String schemaName,
                                  String tableName) throws SQLException {
        //We filter just VIEW, TABLE and SYNONYM. For more table types: connection.getMetaData().getTableTypes()
        return connection.getMetaData().getTables(null, schemaName, tableName, new String[] {"VIEW", "TABLE", "SYNONYM"});
    }

    @Nullable
    @Override
    public JdbcTableHandle getTableHandle(SchemaTableName schemaTableName) {
        Connection connection = null;
        try{
            connection = driver.connect(connectionUrl, connectionProperties);
            DatabaseMetaData metadata = connection.getMetaData();
            String jdbcSchemaName = schemaTableName.getSchemaName();
            String jdbcTableName = schemaTableName.getTableName();
            if (metadata.storesUpperCaseIdentifiers()) {
                jdbcSchemaName = jdbcSchemaName.toUpperCase();
                jdbcTableName = jdbcTableName.toUpperCase();
            }
            ResultSet resultSet = getTables(connection, jdbcSchemaName, jdbcTableName);
            List<JdbcTableHandle> tableHandles = new ArrayList<JdbcTableHandle>();

            while (resultSet.next()) {
                log.info(String.format("[%s]Schema names: - START", connectorId));//TODO delete
                int columns = resultSet.getMetaData().getColumnCount();
                for(int j=1;j<=columns;j++){
                    log.info(String.format("[%s]: %s", resultSet.getMetaData().getColumnName(j), resultSet.getObject(j)));
                }
                log.info(String.format("[%s]Schema names: - END", connectorId));
                tableHandles.add(
                        new JdbcTableHandle(
                                connectorId,
                                schemaTableName,
                                resultSet.getString("TABLE_CAT"),
                                resultSet.getString("TABLE_SCHEM"),
                                resultSet.getString("TABLE_NAME")
                        )
                );
            }
            if (tableHandles.isEmpty()) {
                return null;
            }
            if (tableHandles.size() > 1) {
                throw new PrestoException(NOT_SUPPORTED,
                        "Multiple tables matched: " + schemaTableName);
            }
            return getOnlyElement(tableHandles);
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }finally {
            if(connection!=null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    log.warn("Failed closing connection", e);
                }
            }
        }
    }

    @Override
    public List<JdbcColumnHandle> getColumns(JdbcTableHandle tableHandle) {
        Connection connection = null;
        List<JdbcColumnHandle> columns = new ArrayList<JdbcColumnHandle>();
        try {
            log.info("Listing connection properties - START");//TODO delete
            for(String string : connectionProperties.stringPropertyNames()){
                log.info(String.format("[%s] %s", string, connectionProperties.getProperty(string)));
            }
            log.info("Listing connection properties - END");

            connection = driver.connect(connectionUrl, connectionProperties);
            //we are not able to manage synonims from Teradata
            DatabaseMetaData metadata = connection.getMetaData();
            if(tableHandle!=null){
                if(tableHandle.getSchemaName()==null){
                    throw new SchemaNotFoundException("No schema name!");
                }
                String schemaName = tableHandle.getSchemaName().toUpperCase();
                String tableName = tableHandle.getTableName().toUpperCase();
                ResultSet resultSet = metadata.getColumns(null, schemaName, tableName, null);
                boolean found = false;
                while (resultSet.next()) {
                    found = true;
                    Type columnType = toPrestoType(resultSet.getInt("DATA_TYPE"), resultSet.getInt("COLUMN_SIZE"));
                    // skip unsupported column types
                    if (columnType != null) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        columns.add(new JdbcColumnHandle(connectorId, columnName, columnType));
                    }
                }
                if (!found) {
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                if (columns.isEmpty()) {
                    throw new PrestoException(NOT_SUPPORTED, String.format("Table has no supported column types: %s", tableHandle.getSchemaTableName()));
                }
            }
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }finally {
            if(connection!=null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    log.warn("Failed closing connection", e);
                }
            }
        }
        return ImmutableList.copyOf(columns);
    }

    @Override
    public List<SchemaTableName> getTableNames(@Nullable String schema) {
        Connection connection = null;
        try {
            connection = driver.connect(connectionUrl, connectionProperties);
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers() && (schema != null)) {
                schema = schema.toUpperCase();
            }
            ResultSet resultSet = getTables(connection, schema, null);
            ImmutableList.Builder<SchemaTableName> list = ImmutableList.builder();
            while (resultSet.next()) {
                list.add(getSchemaTableName(resultSet));
            }
            return list.build();
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }finally {
            if(connection!=null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    log.warn("Failed closing connection", e);
                }
            }
        }
    }

    @Override
    protected SchemaTableName getSchemaTableName(ResultSet resultSet)
            throws SQLException {
        String tableSchema = resultSet.getString("TABLE_SCHEM");
        String tableName = resultSet.getString("TABLE_NAME");
        if (tableSchema != null) {
            tableSchema = tableSchema.toLowerCase();
        }
        if (tableName != null) {
            tableName = tableName.toLowerCase();
        }
        return new SchemaTableName(tableSchema, tableName);
    }

    @Override
    public PreparedStatement buildSql(Connection connection, JdbcSplit split, List<JdbcColumnHandle> columnHandles) throws SQLException {
        log.info("We are debugging how sql is built!");//TODO delete
        log.info(String.format("[split]: %s", split));
        return super.buildSql(connection, split, columnHandles);
    }

    @Override
    protected void execute(Connection connection, String query) throws SQLException {
        //TODO analyze sql query and eventually translate into Teradata dialect
        log.info(String.format("We just received the following query for execution: %s", query));
        super.execute(connection, rewriteQuery(query));
    }

    @VisibleForTesting
    protected String rewriteQuery(String query){
        return query;
    }

}