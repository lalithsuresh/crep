package com.absinthe.crep;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by lalithsuresh on 2/18/15.
 */
public class Scheduler {

    private final ArrayList<ClientThread> clientThreads;
    private final Random rand = new Random();

    Scheduler (ArrayList<ClientThread> clientThreads) {
        this.clientThreads = clientThreads;
    }

    public void schedule(Request req) {
        ClientThread exec = clientThreads.get(rand.nextInt(clientThreads.size()));
        exec.enqueue(req);
    }
}
