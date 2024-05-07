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

import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.airlift.log.Logger;
import io.trino.plugin.hive.HiveConnectorFactory.EmptyModule;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

public class DeltaLakePlugin
        implements Plugin
{
    private static final Logger LOG = Logger.get(DeltaLakePlugin.class);

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        LOG.info("Getting connector factories in DeltaLakePlugin - melody");
        return ImmutableList.of(getConnectorFactory(EmptyModule.class));
    }

    public ConnectorFactory getConnectorFactory(Class<? extends Module> module)
    {
        return new DeltaLakeConnectorFactory(module);
    }
}
