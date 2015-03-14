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

import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.*;

import com.netflix.astyanax.model.*;
import com.netflix.astyanax.retry.RunOnce;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.apache.log4j.Logger;


/**
 * Created by lalithsuresh on 2/18/15.
 */

class StatusThread extends Thread {

    private final List<ClientThread> clientThreads;
    private final int statusCheckInterval; // ms

    private volatile long totalOps = -1;
    private volatile boolean terminate = false;

    StatusThread (List<ClientThread> clientThreads,
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

        while (!terminate || totalOps != completedOps) {
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
            t.shutDown();
        }
    }

    public void receiveTerminateCondition(long totalOps) {
        this.totalOps = totalOps;
        this.terminate = true;
    }
}

public class ClientThread extends Thread {

    private final BlockingQueue<Request> taskQueue = new LinkedBlockingQueue<Request>();
    private volatile boolean terminate = false;
    private final ClientDriver driver;

    ClientThread(ClientDriver driver) {
        this.driver = driver;
    }

    @Override
    public void run() {
        while (terminate == false) {
            Request req = null;
            try {
                req = taskQueue.take();
            } catch (InterruptedException e) {
                System.out.println("Closing thread " + currentThread().getName() + " on interrupt");
                return;
            }

            if (req instanceof ReadRequest) {
                read((ReadRequest) req);
            }
            else if (req instanceof InsertRequest) {
                insert((InsertRequest) req);
            }
        }
    }

    public void insert(InsertRequest request) {
        driver.insert(request);
    }

    public void read(ReadRequest request) {
        driver.read(request);
    }

    public void enqueue(Request req) {
        try {
            taskQueue.put(req);
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    public int getTotalCompletedOps() {
        return driver.getTotalCompletedOps();
    }

    public void shutDown() {
        driver.shutDown();
    }

    public static void main(String [] args) {
        Conf conf = Conf.getConf("conf/crep.yaml");
        AstyanaxDriver.init(conf);

        ArrayList<ClientThread> clientThreads = new ArrayList<>();

        for (int i = 0; i < conf.num_client_threads; i++) {
            ClientDriver driver = new AstyanaxDriver();
            ClientThread ct = new ClientThread(driver);
            clientThreads.add(ct);
            ct.start();
        }

        StatusThread statusThread = new StatusThread(clientThreads,
                                                     conf.status_thread_update_interval_ms);
        statusThread.start();

        Scheduler scheduler = new Scheduler(clientThreads);
        Scenario.setColumnNames(conf.schema_file);
        long totalOps = 0;
        if (conf.workload_type.equals(Conf.WorkloadType.FILE))
            totalOps = Scenario.executeFromFile(conf.workload_file, scheduler, conf);
        else if (conf.workload_type.equals(Conf.WorkloadType.SYNTHETIC))
            totalOps = Scenario.executeSynthentic(scheduler, conf);
        else {
            throw new AssertionError("Not generating scenario");
        }
        statusThread.receiveTerminateCondition(totalOps);
    }
}
