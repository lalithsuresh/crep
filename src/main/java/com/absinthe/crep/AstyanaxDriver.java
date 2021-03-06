package com.absinthe.crep;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.*;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.retry.RunOnce;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.apache.log4j.*;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by lalith on 09.03.15.
 */
public class AstyanaxDriver extends ClientDriver {

    static Logger logger = LogManager.getLogger(ClientThread.class);

    private static AstyanaxContext context;
    private static Keyspace keyspace;
    private static ColumnFamily CF;
    private static double statsSampleChance = 1.0;

    private Random rand = new Random();

    public static void init(Conf conf) {
        CountingConnectionPoolMonitor monitor = new CountingConnectionPoolMonitor();
        String cluster_name = conf.cluster_name;
        String keyspaceName = conf.keyspace_name;
        String columnfamilyName = conf.column_family_name;
        int maxconns = conf.max_conns;
        String hosts = conf.hosts;
        String connection_pool_type = conf.connection_pool_type;
        System.out.println(hosts);

        context = new AstyanaxContext.Builder()
                .forCluster(cluster_name)
                .forKeyspace(keyspaceName)
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                                .setDiscoveryType(NodeDiscoveryType.NONE)
                                .setConnectionPoolType(ConnectionPoolType.valueOf(connection_pool_type))
                                .setDefaultWriteConsistencyLevel(ConsistencyLevel.CL_QUORUM)
                                .setDefaultReadConsistencyLevel(ConsistencyLevel.CL_ONE)
                                .setRetryPolicy(RunOnce.get())
                )
                .withConnectionPoolConfiguration(

                        new ConnectionPoolConfigurationImpl(cluster_name)
                                .setSeeds(hosts)
                                .setMaxConnsPerHost(maxconns)
                                .setMaxConns(maxconns * (hosts.split(",").length))
                                .setSocketTimeout(10000)
                                .setLatencyScoreStrategy(new SmaLatencyScoreStrategyImpl(
                                        ConnectionPoolConfigurationImpl.DEFAULT_LATENCY_AWARE_UPDATE_INTERVAL,
                                        ConnectionPoolConfigurationImpl.DEFAULT_LATENCY_AWARE_RESET_INTERVAL,
                                        ConnectionPoolConfigurationImpl.DEFAULT_LATENCY_AWARE_WINDOW_SIZE,
                                        ConnectionPoolConfigurationImpl.DEFAULT_LATENCY_AWARE_BADNESS_THRESHOLD
                                ))
                )
                .withConnectionPoolMonitor(monitor)
                .buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();
        keyspace = (Keyspace) context.getClient();
        CF = ColumnFamily.newColumnFamily(columnfamilyName, StringSerializer.get(), StringSerializer.get());
        statsSampleChance = conf.stats_sample_chance;
    }

    public static synchronized void shutDown() {
        ExecutorService executor = context.getAstyanaxConfiguration().getAsyncExecutor();

        if (!executor.isShutdown()) {
            executor.shutdown();
            try {
                while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                }
            } catch (InterruptedException e) {
                // fall through
            }
            context.shutdown();
        }
    }

    public void AstyanaxDriver() {
    }

    @Override
    public void read(ReadRequest request) {
        try {
            OperationResult<Rows<String, String>> result =
                    keyspace.prepareQuery(CF).getKeySlice(request.keys).execute();

            if (result != null) {
                totalCompletedOps += 1;
                totalCompletedReads += result.getResult().size();

                if (statsSampleChance >= rand.nextDouble()) {
                    logger.info("Read " + request.keys.size() + " " + result.getResult().size() + " " + result.getLatency());
                }
            }
        } catch (ConnectionException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void insert(InsertRequest request) {
        MutationBatch mb = keyspace.prepareMutationBatch();

        for (Map.Entry<String, Map<String, Integer>> row: request.mutations.entrySet()) {
            // Each entry is a key and a map of field -> values
            ColumnListMutation mutation = mb.withRow(CF, row.getKey());   // Key
            for (Map.Entry<String, Integer> columnMutation: row.getValue().entrySet()) {
                mutation = mutation.putColumn(columnMutation.getKey(),    // Field name
                        columnMutation.getValue()); // Field value
            }
        }
        try {
            OperationResult<Void> result = mb.execute();
            if (result != null) {
                totalCompletedOps += 1;
                totalCompletedWrites += request.mutations.size();

                if (statsSampleChance >= rand.nextFloat()) {
                    logger.info("Insert " + request.mutations.size() + " " + result.getResult() + " " + result.getLatency());
                }
            }
        } catch (ConnectionException e) {
            e.printStackTrace();
        }

    }
}
