package com.absinthe.crep;

import java.util.ArrayList;
import java.util.Random;

/**
 * Created by lalithsuresh on 2/18/15.
 */
public class Scheduler {

    private final ArrayList<Executor> executors  = new ArrayList<Executor>();
    private final Random rand = new Random();

    Scheduler () {

    }

    public void schedule(Request req) {
        Executor exec = executors.get(rand.nextInt(executors.size()));
        exec.enqueue(req);
    }
}
