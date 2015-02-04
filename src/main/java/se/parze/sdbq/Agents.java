package se.parze.sdbq;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Agents<E extends Agent> {

    private List<E> agents;

    public Agents() {
        this.agents = new ArrayList<E>();
    }

    public List<E> getAgents() {
        return this.agents;
    }

    public void addAgent(E agent) {
        this.agents.add(agent);
    }

    public void startAgents() {
        for (Agent agent : this.agents) {
            agent.startAgent();
        }
    }

    public Date getWorkLastFinished() {
        Date workLastFinished = new Date(0);
        for (Agent agent : this.agents) {
            if (agent.getWorkLastFinished().after(workLastFinished)) {
                workLastFinished = agent.getWorkLastFinished();
            }
        }
        return workLastFinished;
    }

    public void waitUntilAllAgentsAreDone() {
        for (Agent agent : this.agents) {
            agent.waitUntilAllWorkIsDone();
        }
    }

    public void notifyAgentsThatWorkIsReadyForProcessing() {
        for (Agent agent : this.agents) {
            agent.notifyAgentThatWorkIsReadyForProcessing();
        }
    }

}
