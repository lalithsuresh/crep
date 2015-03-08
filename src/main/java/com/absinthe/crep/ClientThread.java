package com.absinthe.crep;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.impl.SmaLatencyScoreStrategyImpl;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;

import java.util.*;
import java.util.concurrent.*;

import com.netflix.astyanax.model.*;
import com.netflix.astyanax.retry.RunOnce;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 * Created by lalithsuresh on 2/18/15.
 */

class StatusThread extends Thread {

    private final List<ClientThread> clientThreads;
    private final int totalOps;
    private final int statusCheckInterval; // ms

    StatusThread (List<ClientThread> clientThreads,
                  int totalOps,
                  int statusCheckInterval) {
        assert clientThreads != null;
        assert totalOps > 0;
        assert statusCheckInterval >= 1;
        this.clientThreads = clientThreads;
        this.totalOps = totalOps;
        this.statusCheckInterval = statusCheckInterval;
    }

    @Override
    public void run() {
        int completedOps = 0;
        int lastCompletedOps = 0;
        double throughput;

        while (totalOps != completedOps) {
            lastCompletedOps = completedOps;
            completedOps = 0;

            try {
                Thread.sleep(statusCheckInterval);
            } catch (InterruptedException e) {
                new AssertionError("Status thread interrupted");
            }
            for (ClientThread t: clientThreads) {
                completedOps += t.getTotalCompletedOps();
            }

            throughput = ((double) (completedOps - lastCompletedOps)
                    /(double) statusCheckInterval) * 1000;
            System.out.println("Status thread, Completed Ops: "
                                + completedOps + ", Throughput: " + throughput);
        }

        for (ClientThread t: clientThreads) {
            t.interrupt();
        }

        ClientThread.shutdownAstyanax();
    }
}

public class ClientThread extends Thread {


    private static AstyanaxContext context;
    private static Keyspace keyspace;
    private static ColumnFamily CF;

    private final BlockingQueue<Request> taskQueue = new LinkedBlockingQueue<Request>();
    private volatile boolean terminate = false;
    private final Conf conf;
    private volatile int totalCompletedOps = 0;


    public static void setupAstyanaxEnv(Conf conf) {
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

    ClientThread(Conf conf) {
        this.conf = conf;
    }

    public static void shutdownAstyanax() {
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
                System.out.println("Closing thread " + currentThread().getName() + " on interrupt");
                return;
            }

            if (req instanceof ReadRequest) {
                System.out.println(((ReadRequest) req).keys);
                read((ReadRequest) req);
            }
            else if (req instanceof InsertRequest) {
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
            totalCompletedOps += 1;
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
//                System.out.println("Here: " + result.getResult().getRowByIndex(0).getKey());
                totalCompletedOps += 1;
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

    public int getTotalCompletedOps() {
        return totalCompletedOps;
    }

    public static void main(String [] args) {
        Conf conf = Conf.getConf("conf/crep.yaml");
        ClientThread.setupAstyanaxEnv(conf);
        int totalOps = conf.total_operations;

        ArrayList<ClientThread> clientThreads = new ArrayList<>();

        for (int i = 0; i < conf.num_client_threads; i++) {
            ClientThread ct = new ClientThread(conf);
            clientThreads.add(ct);
            ct.start();
        }

        StatusThread statusThread = new StatusThread(clientThreads,
                                                     totalOps,
                                                     conf.status_thread_update_interval_ms);
        statusThread.start();

        Scheduler scheduler = new Scheduler(clientThreads);
        Scenario.execute(conf.workload_file, scheduler, conf);
    }
}
