
public class UDPPinger implements Runnable {

    int pingInt;
    p2p controller;

    public UDPPinger(int pingInt, p2p controller) {
        this.pingInt = pingInt;
        this.controller = controller;
    }

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

    public void checkForDeadNodes() {
        // else if because no way to handle all known nodes dying at once
        int fstSuccessor = controller.getFstSuccessor();
        int sndSuccessor = controller.getSndSuccessor();
        if (controller.fstSuccessorMissedPingCount() == 4) {
            System.out.println("Peer " + fstSuccessor + " is no longer alive");
            controller.tcpSender(sndSuccessor, controller.DEAD_NODE_DETECTED + fstSuccessor + " From: " + controller.getPeerId());
        } else if (controller.sndSuccessorMissedPingCount() == 4) {
            System.out.println("Peer " + sndSuccessor + " is no longer alive");
            controller.tcpSender(fstSuccessor, controller.DEAD_NODE_DETECTED + sndSuccessor + " From: " + controller.getPeerId());
        }
    }
}