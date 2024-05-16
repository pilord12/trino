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

package io.trino.plugin.deltalake;

import io.trino.plugin.deltalake.metastore.DeltaLakeMetastore;
import io.trino.plugin.deltalake.metastore.DeltaMetastoreTable;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Optional;

public class MelodyDeltaLakeMetastore
        implements DeltaLakeMetastore
{
    @Override
    public List<String> getAllDatabases()
    {
        return List.of();
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        return Optional.empty();
    }

    @Override
    public List<String> getAllTables(String databaseName)
    {
        return List.of();
    }

    @Override
    public Optional<Table> getRawMetastoreTable(String databaseName, String tableName)
    {
        return Optional.empty();
    }

    @Override
    public Optional<DeltaMetastoreTable> getTable(String databaseName, String tableName)
    {
        return Optional.empty();
    }

    @Override
    public void createDatabase(Database database)
    {
        throw new UnsupportedOperationException("Database create operations are not supported");
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        throw new UnsupportedOperationException("Database delete operations are not supported");
    }

    @Override
    public void createTable(ConnectorSession session, Table table, PrincipalPrivileges principalPrivileges)
    {
        throw new UnsupportedOperationException("Table create operations are not supported");
    }

    @Override
    public void dropTable(ConnectorSession session, SchemaTableName schemaTableName, String tableLocation, boolean deleteData)
    {
        throw new UnsupportedOperationException("Table delete operations are not supported");
    }

    @Override
    public void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to)
    {
        throw new UnsupportedOperationException("Table rename operations are not supported");
    }
}
