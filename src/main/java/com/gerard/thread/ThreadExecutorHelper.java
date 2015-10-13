package com.gerard.thread;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Class:
 * Description:
 * <p/>
 * Created by: geal0913
 * Date: 13.04.2015
 */
public class ThreadExecutorHelper {
    private static final Log LOG = LogFactory.getLog(ThreadExecutorHelper.class);

    public static void doInSingleThreadPool(Callable<Void> callable) {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        pool.submit(callable);
    }

    public static <T> void doInSingleThreadPoolWaitForAnswer(Callable<T> callable, int timeout, TimeUnit timeoutMeasure) throws Throwable {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<T> future = pool.submit(callable);

        List<Future<T>> ffs = new ArrayList<Future<T>>();
        ffs.add(future);

        try {
            waitForVoidAnswer(ffs, timeout, timeoutMeasure);
        } finally {
            if (!pool.isShutdown()) {
                pool.shutdown();
            }
        }
    }

    public static <T> void doInSingleThreadPoolWaitForAnswer(Callable<T> callable) throws Throwable {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<T> future = pool.submit(callable);

        List<Future<T>> ffs = new ArrayList<Future<T>>();
        ffs.add(future);

        try {
            waitForVoidAnswer(ffs);
        } finally {
            if (!pool.isShutdown()) {
                pool.shutdown();
            }
        }
    }

    public static <V> void doInFixedThreadPoolWaitForAnswers(List<Callable<V>> callables, int timeout, TimeUnit timeoutMeasure) throws Throwable {
        ExecutorService pool = Executors.newFixedThreadPool(callables.size());
        List<Future<V>> results = pool.invokeAll(callables);

        LOG.debug("Futures obtained --> pool will shutdown now");

        try {
            waitForVoidAnswer(results, timeout, timeoutMeasure);
        } finally {
            if (!pool.isShutdown()) {
                pool.shutdown();
            }
        }
    }

    public static <V> void doInFixedThreadPoolWaitForAnswers(List<Callable<V>> callables) throws Throwable {
        ExecutorService pool = Executors.newFixedThreadPool(callables.size());
        List<Future<V>> results = pool.invokeAll(callables);

        LOG.debug("Futures obtained --> pool will shutdown now");

        try {
            waitForVoidAnswer(results);
        } finally {
            if (!pool.isShutdown()) {
                pool.shutdown();
            }
        }
    }

    private static <T> void waitForVoidAnswer(List<Future<T>> futures) throws Throwable {
        Throwable th = null;

        for (Future<T> ff : futures) {
            try {
                ff.get();
            } catch (ExecutionException e) {
                th = e.getCause();
                break;
            }
        }

        if (th != null) {
            throw th;
        }
    }

    private static <T> void waitForVoidAnswer(List<Future<T>> futures, int timeout, TimeUnit timeoutMeasure) throws Throwable {
        Throwable th = null;

        for (Future<T> ff : futures) {
            try {
                ff.get(timeout, timeoutMeasure);
            } catch (ExecutionException e) {
                th = e.getCause();
                break;
            }
        }

        if (th != null) {
            throw th;
        }
    }
}
