package se.parze.tdbq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public abstract class Worker {

    private Logger logger = LoggerFactory.getLogger(Worker.class);

    private WorkerThread workerThread;

    private Object notifiesWhenWorkerHasNewWork = new Object();

    private Object notifiesWhenAllWorkIsDone = new Object();

    private boolean shouldBeActive;

    private boolean workIsPendingForProcessing = false;

    private boolean isWaiting = false;

    private Date workLastFinished = null;

    private String name;

    public Worker(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public void startWorker() {
        this.workerThread = new WorkerThread();
        this.workerThread.setDaemon(true);
        this.shouldBeActive = true;
        this.workerThread.start();
        logger.info("Started Worker " + getName());
    }

    public void stopWorker() {
        shouldBeActive = false;
    }

    public Date getWorkLastFinished() {
        return workLastFinished;
    }

    public void notifyWorkerThatWorkIsReadyForProcessing() {
        synchronized (notifiesWhenWorkerHasNewWork) {
            workIsPendingForProcessing = true;
            notifiesWhenWorkerHasNewWork.notify();
        }
    }

    public void waitUntilAllWorkIsDone() {
        if (!workIsPendingForProcessing && isWaiting) {
            return;
        }
        try {
            synchronized (notifiesWhenAllWorkIsDone) {
                notifiesWhenAllWorkIsDone.wait();
            }
        } catch (InterruptedException e) {}
    }

    public abstract void computeWork();


    public class WorkerThread extends Thread {
        @Override
        public void run() {
            while (shouldBeActive) {
                synchronized (notifiesWhenWorkerHasNewWork) {
                    if (!workIsPendingForProcessing) {
                        synchronized (notifiesWhenAllWorkIsDone) {
                            notifiesWhenAllWorkIsDone.notifyAll();
                        }
                        try {
                            isWaiting = true;
                            notifiesWhenWorkerHasNewWork.wait();
                            isWaiting = false;
                        } catch (InterruptedException e) {}
                    }
                    workIsPendingForProcessing = false;
                }
                try {
                    computeWork();
                } catch (Throwable e) {
                    logger.error("Exception was thrown when worker "+getName()+" computing work.", e);
                }
                workLastFinished = new Date();
            }
        }

    }

}
