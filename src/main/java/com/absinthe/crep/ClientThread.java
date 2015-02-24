package com.absinthe.crep;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.impl.SmaLatencyScoreStrategyImpl;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;

import java.nio.ByteBuffer;
import java.security.Key;
import java.util.*;
import java.util.concurrent.*;

import com.netflix.astyanax.model.*;
import com.netflix.astyanax.retry.RunOnce;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 * Created by lalithsuresh on 2/18/15.
 */
public class ClientThread extends Thread {

    private final BlockingQueue<Request> taskQueue = new LinkedBlockingQueue<Request>();
    private volatile boolean terminate = false;

    private AstyanaxContext context;
    private Keyspace keyspace;
    private ColumnFamily CF;
    private final Conf conf;
    private int total = 0;

    ClientThread(Conf conf) {
        this.conf = conf;
    }

    public void init() throws Exception {
    }

    public void setupAstyanaxEnv(Conf conf) {
        CountingConnectionPoolMonitor monitor = new CountingConnectionPoolMonitor();
        ExecutorService executor = Executors.newFixedThreadPool(conf.async_executor_num_threads);

        String cluster_name = conf.cluster_name;
        String keyspaceName = conf.keyspace_name;
        String columnfamilyName = conf.column_family_name;
        int maxconns = conf.max_conns;
        String hosts = conf.hosts;
        System.out.println(hosts);

        context = new AstyanaxContext.Builder()
                .forCluster(cluster_name)
                .forKeyspace(keyspaceName)
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                                .setDiscoveryType(NodeDiscoveryType.NONE)
                                .setAsyncExecutor(executor)
                                .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN)
                                .setDefaultWriteConsistencyLevel(ConsistencyLevel.CL_ONE)
                                .setDefaultReadConsistencyLevel(ConsistencyLevel.CL_ONE)
                                .setRetryPolicy(RunOnce.get())
                )
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl(cluster_name)
                                .setMaxConnsPerHost(maxconns)
                                .setSeeds(hosts)
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
    }

    public void shutdown() {
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

    @Override
    public void run() {
        setupAstyanaxEnv(this.conf);

        while (terminate == false) {
            Request req = null;
            try {
                req = taskQueue.take();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            // XXX: issue calls to Cassandra here.
            if (req instanceof ReadRequest) {
                // XXX: issue reads
                System.out.println(((ReadRequest) req).keys);
                read((ReadRequest) req);
            }
            else if (req instanceof InsertRequest) {
                // XXX: issue insert
                insert((InsertRequest) req);
            }
        }
    }

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
        } catch (ConnectionException e) {
            e.printStackTrace();
        }
    }

    public void read(ReadRequest request) {
        try {
            OperationResult<Rows<String, String>> result =
                    keyspace.prepareQuery(CF).getKeySlice(request.keys).execute();
            if (result != null) {
                System.out.println(result.getResult().getRowByIndex(0).getKey());
                total += 1;
            }

        } catch (ConnectionException e) {
            e.printStackTrace();
        }
    }

    public void enqueue(Request req) {
        try {
            taskQueue.put(req);
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    public static void main(String [] args) {
        Conf conf = new Conf("conf/crep.yaml");
        ClientThread ct = new ClientThread(conf);
        ArrayList<ClientThread> clientThreads = new ArrayList<>();
        clientThreads.add(ct);
        Scheduler scheduler = new Scheduler(clientThreads);
        ct.start();

//        Map<String, Map<String, Integer>> mutations = new HashMap<>();
//        mutations.put("k1", new HashMap<String, Integer>());
//        mutations.put("k2", new HashMap<String, Integer>());
//        mutations.put("k3", new HashMap<String, Integer>());
//        mutations.get("k1").put("field1", 15);
//        mutations.get("k1").put("field2", 16);
//        mutations.get("k1").put("field3", 17);
//        mutations.get("k2").put("field1", 37);
//        mutations.get("k3").put("field1", 58);
//        InsertRequest insertRequest = new InsertRequest("data", mutations);
//        ct.insert(insertRequest);
//
        List<String> keyList = new ArrayList<String>();
        keyList.add("k1");
        keyList.add("k2");
        keyList.add("k3");
        int tries = 1000;
        long start = System.currentTimeMillis();

        while (tries != 0) {
//            ct.read(req);
            ReadRequest req = new ReadRequest("data", keyList, null);
            scheduler.schedule(req);
            tries -= 1;
            System.out.println(tries);
        }
        System.out.println("Done : " + (System.currentTimeMillis() - start));

//        ct.shutdown();
    }
}
