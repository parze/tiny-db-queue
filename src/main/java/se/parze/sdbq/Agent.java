package se.parze.sdbq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public abstract class Agent {

    private Logger logger = LoggerFactory.getLogger(Agent.class);

    private Worker worker;

    private Object notifiesWhenAgentHasNewWork = new Object();

    private Object notifiesWhenAllWorkIsDone = new Object();

    private boolean shouldBeActive;

    private boolean workIsPendingForProcessing = false;

    private boolean isWaiting = false;

    private Date workLastFinished = null;

    private String name;

    public Agent(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public void startAgent() {
        this.worker = new Worker();
        this.worker.setDaemon(true);
        this.shouldBeActive = true;
        this.worker.start();
        logger.info("Started agent " + getName());
    }

    public void stopAgent() {
        shouldBeActive = false;
    }

    public Date getWorkLastFinished() {
        return workLastFinished;
    }

    public void notifyAgentThatWorkIsReadyForProcessing() {
        synchronized (notifiesWhenAgentHasNewWork) {
            workIsPendingForProcessing = true;
            notifiesWhenAgentHasNewWork.notify();
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


    public class Worker extends Thread {
        @Override
        public void run() {
            while (shouldBeActive) {
                synchronized (notifiesWhenAgentHasNewWork) {
                    if (!workIsPendingForProcessing) {
                        synchronized (notifiesWhenAllWorkIsDone) {
                            notifiesWhenAllWorkIsDone.notifyAll();
                        }
                        try {
                            isWaiting = true;
                            notifiesWhenAgentHasNewWork.wait();
                            isWaiting = false;
                        } catch (InterruptedException e) {}
                    }
                    workIsPendingForProcessing = false;
                }
                try {
                    computeWork();
                } catch (Throwable e) {
                    logger.error("Exception was thrown when agent "+getName()+" computing work.", e);
                }
                workLastFinished = new Date();
            }
        }

    }

}
