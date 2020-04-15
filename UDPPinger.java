
public class UDPPinger implements Runnable {

    int pingInt;
    p2p controller;

    public UDPPinger(int pingInt, p2p controller) {
        this.pingInt = pingInt;
        this.controller = controller;
    }

    public void run() {
        while(true){
            controller.pingS(controller.ASK_STILL_ALIVE + " fstSuccessor", controller.getFstSuccSocket());
            controller.pingS(controller.ASK_STILL_ALIVE + " sndSuccessor", controller.getSndSuccSocket());
            System.out.println("Ping requests sent to Peers " + controller.getFstSuccessor() + " and " + controller.getSndSuccessor());
            controller.sndSuccessorMissedPingsIncrement();
            controller.fstSuccessorMissedPingsIncrement();
            try {
                Thread.sleep(pingInt);
            } catch (InterruptedException e) {
                System.out.println(e);
            }
        }
    }
}