package se.parze.sdbq;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Workers<E extends Worker> {

    private List<E> workers;

    public Workers() {
        this.workers = new ArrayList<E>();
    }

    public List<E> getWorkers() {
        return this.workers;
    }

    public void addWorker(E worker) {
        this.workers.add(worker);
    }

    public void startWorkers() {
        for (Worker worker : this.workers) {
            worker.startWorker();
        }
    }

    public Date getWorkLastFinished() {
        Date workLastFinished = new Date(0);
        for (Worker worker : this.workers) {
            if (worker.getWorkLastFinished().after(workLastFinished)) {
                workLastFinished = worker.getWorkLastFinished();
            }
        }
        return workLastFinished;
    }

    public void waitUntilAllWorkersAreDone() {
        for (Worker worker : this.workers) {
            worker.waitUntilAllWorkIsDone();
        }
    }

    public void notifyWorkersThatWorkIsReadyForProcessing() {
        for (Worker worker : this.workers) {
            worker.notifyWorkerThatWorkIsReadyForProcessing();
        }
    }

}
