
public class UDPPinger implements Runnable {

    int pingInt;
    p2p controller;

    public UDPPinger(int pingInt, p2p controller) {
        if (pingInt < 100) pingInt = 10000;
        this.pingInt = pingInt;
        this.controller = controller;
    }

    /**
     * Pings successors every x milliseconds (specified by ping int)
     * Increments missed ping counters when ping is sent
     * Checks if a node can be considered dead between pings
     */
    public void run() {
        while(true) {
            controller.pingS(controller.ASK_STILL_ALIVE + " fstSuccessor", controller.getFstSuccSocket());
            controller.pingS(controller.ASK_STILL_ALIVE + " sndSuccessor", controller.getSndSuccSocket());
            System.out.println("Ping requests sent to Peers " + controller.getFstSuccessor() + " and " + controller.getSndSuccessor());
            controller.sndSuccessorMissedPingsIncrement();
            controller.fstSuccessorMissedPingsIncrement();
            try {
                Thread.sleep(100);
                checkForDeadNodes();
                Thread.sleep(pingInt - 100);
            } catch (InterruptedException e) {
                System.out.println(e);
            }
        }
    }

    
    /**
     * Checkes if there are any dead nodes in the network
     * A node is considered dead if they have missed 4 pings in a row
     */
    public void checkForDeadNodes() {
        int fstSuccessor = controller.getFstSuccessor();
        int sndSuccessor = controller.getSndSuccessor();
        if (controller.fstSuccessorMissedPingCount() == 4) {
            System.out.println("Peer " + fstSuccessor + " is no longer alive");
            controller.tcpSender(sndSuccessor, controller.DEAD_NODE_DETECTED + fstSuccessor + " From: " + controller.getPeerId());
        } 
        if (controller.sndSuccessorMissedPingCount() == 4) {
            System.out.println("Peer " + sndSuccessor + " is no longer alive");
            controller.tcpSender(fstSuccessor, controller.DEAD_NODE_DETECTED + sndSuccessor + " From: " + controller.getPeerId());
        }
    }
}