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
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
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
        String jwt = ""; // populate from CLI metadata

        CloseableHttpClient client = HttpClientBuilder.create().build();
        var request = new HttpPost("https://dev.dataplatform.hpedev.net/dev/data-catalog/organizations/octo/domains/harmony-dev/assets");

        request.addHeader("Content-Type", "application/json");
        request.addHeader("Authorization", "Bearer " + jwt);
        ArrayList<String> tables = new ArrayList<>();

        try {
            CloseableHttpResponse response = client.execute(request);
            JSONArray json = new JSONArray(EntityUtils.toString(response.getEntity()));
            for (int i = 0; i < json.length(); i++) {
                JSONObject o = json.getJSONObject(i);
                tables.add(o.getString("name"));
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        return tables;
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
        throw new UnsupportedOperationException("Write operations are not supported");
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        throw new UnsupportedOperationException("Write operations are not supported");
    }

    @Override
    public void createTable(ConnectorSession session, Table table, PrincipalPrivileges principalPrivileges)
    {
        throw new UnsupportedOperationException("Write operations are not supported");
    }

    @Override
    public void dropTable(ConnectorSession session, SchemaTableName schemaTableName, String tableLocation, boolean deleteData)
    {
        throw new UnsupportedOperationException("Write operations are not supported");
    }

    @Override
    public void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to)
    {
        throw new UnsupportedOperationException("Write operations are not supported");
    }
}
