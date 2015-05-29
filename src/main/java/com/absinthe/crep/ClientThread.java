package com.absinthe.crep;

import java.util.*;
import java.util.concurrent.*;


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
        long completedOps = 0;
        long lastCompletedOps = 0;
        double opsThroughput;

        long completedReads = 0;
        long lastCompletedReads = 0;
        double readThroughput = 0;

        while (!terminate || totalOps != completedOps) {
            long start = System.currentTimeMillis();
            lastCompletedOps = completedOps;
            completedOps = 0;

            lastCompletedReads = completedReads;
            completedReads = 0;

            try {
                Thread.sleep(statusCheckInterval);
            } catch (InterruptedException e) {
                new AssertionError("Status thread interrupted");
            }
            for (ClientThread t: clientThreads) {
                completedOps += t.getTotalCompletedOps();
                completedReads += t.getTotalCompletedReads();
            }

            opsThroughput = ((double) (completedOps - lastCompletedOps)
                    /(double) (System.currentTimeMillis() - start)) * 1000;
            readThroughput = ((double) (completedReads - lastCompletedReads)
                    /(double) (System.currentTimeMillis() - start)) * 1000;

            System.out.println("Status thread, Completed Ops: " + completedOps +
                               ", Ops-Throughput: " + opsThroughput +
                               ", Read-throughput: " + readThroughput);

            Scenario.canProceed(true, completedOps, opsThroughput * statusCheckInterval/1000.0);
        }

        for (ClientThread t: clientThreads) {
            t.interrupt();
        }
        AstyanaxDriver.shutDown();
    }

    public void receiveTerminateCondition(long totalOps) {
        this.totalOps = totalOps;
        this.terminate = true;
        System.out.println("Received terminate condition for " + this.totalOps + " operations");
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
        try {
            Random r = new Random();
            // Allow the Scenario part of the system to populate
            // request queues for five seconds plus some jitter
            Thread.sleep(5000 + r.nextInt(1000));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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

    public int getTotalCompletedReads() {
        return driver.getTotalCompletedReads();
    }

    public void shutDown() {
        driver.shutDown();
    }

    public static void main(String [] args) throws InterruptedException {
        Conf conf = Conf.getConf("conf/crep.yaml");

        ArrayList<ClientThread> clientThreads = new ArrayList<>();
        AstyanaxDriver.init(conf);
        for (int i = 0; i < conf.num_client_threads; i++) {
            ClientDriver driver = new AstyanaxDriver();
            ClientThread ct = new ClientThread(driver);
            ct.setName("ClientThread-" + i);
            clientThreads.add(ct);
            ct.start();
        }

        StatusThread statusThread = new StatusThread(clientThreads,
                                                     conf.status_thread_update_interval_ms);
        statusThread.setName("StatusThread");
        statusThread.start();

        Scheduler scheduler = new Scheduler(clientThreads);
        Scenario.setColumnNames(conf.schema_file);
        long totalOps = 0;
        if (conf.workload_type.equals(Conf.WorkloadType.WORKLOAD_FILE))
            totalOps = Scenario.executeFromFile(conf.workload_file, scheduler, conf);
        else if (conf.workload_type.equals(Conf.WorkloadType.SYNTHETIC_LOAD))
            totalOps = Scenario.syntheticLoad(scheduler, conf);
        else {
            throw new AssertionError("Not generating scenario");
        }
        statusThread.receiveTerminateCondition(totalOps);
    }
}
