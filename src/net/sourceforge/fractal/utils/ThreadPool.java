package net.sourceforge.fractal.utils;

import java.util.LinkedList;

/**   
 * @author L. Camargos
* 
*/

public class ThreadPool {
    private final int nThreads;
    private PoolWorker[] threads;
    private final LinkedList<Runnable> queue;

    public ThreadPool(int nThreads)
    {
        this.nThreads = nThreads;
        queue = new LinkedList<Runnable>();
    }

    public void doTask(Runnable r) {
        synchronized(queue) {
            queue.addLast(r);
            queue.notify();
        }
    }
    
    public void startThreads() {
        threads = new PoolWorker[nThreads];

        for (int i=0; i < nThreads; i++) {
            threads[i] = new PoolWorker(""+i);
            threads[i].start();
        }
    }
    
    public void stopThreads(){
        for (int i=0; i < nThreads; i++) {
            threads[i].stopThread();
            threads[i] = null;
        }
    }

    private class PoolWorker extends Thread {
        private boolean terminate = false;
        public PoolWorker(String name) {
            super("ThreadPool:Worker-"+name);
            
        }
        
        public void stopThread(){
            terminate = true;
            this.interrupt();
        }

        public void run() {
            Runnable r;

            while (!terminate) {
                synchronized(queue) {
                    while (queue.isEmpty()&!terminate) {
                        try{
                            queue.wait();
                        }catch (InterruptedException ignored){}
                    }
                    if(terminate)
                        break;
                    r = (Runnable) queue.removeFirst();
                }


                // If we don't catch RuntimeException, 
                // the pool could leak threads
                try {
                    r.run();
                    yield();
                }catch (RuntimeException e) {e.printStackTrace();}
            }
        }
    }
}