package net.sourceforge.fractal.utils;

public class SuicidalExceptionHandler implements Thread.UncaughtExceptionHandler{
    public void uncaughtException(Thread thr, Throwable exc) {
        System.err.println("Thread " +thr.getName() + " is commiting suicide\n"+exc.toString());
        exc.printStackTrace();
        System.exit(0xF00D);
    }    
}
