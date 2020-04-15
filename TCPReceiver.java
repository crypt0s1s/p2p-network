
import java.io.*;
import java.net.*;
import java.util.*;
import java.text.SimpleDateFormat;
import java.util.concurrent.locks.*;

public class TCPReceiver implements Runnable {

    int peerID;
    p2p controller;

    final String JOIN_REQ_ACCEPTED = "Join request has been accepted"; 
    final String CHANGE_REQ_RECEIVED = "Successor Change request received";
    final String SUCCESSOR_CONFIRM = "You are my first successor ";

    public TCPReceiver(int peerID, p2p controller) {
        this.peerID = peerID;
        this.controller = controller;
    }

    /**
     * Updates the values of fst and snd successor
     * @param newFst New fst successor ID
     * @param newSnd New snd successor ID
     */
    private void updateSuccesssors(int newFst, int newSnd) {
        controller.setFstSuccessor(newFst);
        System.out.println("My new first successor is Peer " + newFst);
        controller.setSndSuccessor(newSnd);
        System.out.println("My new second successor is Peer " + newSnd);
    }

    /**
     * setSuccessors is used by a node joining a network
     * It sets the successors to the right values 
     * @param arr arguments of a message recieved containing details about its successors
     */
    private void setSuccessors(String[] arr) {
        System.out.println(JOIN_REQ_ACCEPTED);
        controller.setFstSuccessor(Integer.parseInt(arr[2]));
        System.out.println("My first successor is Peer " + arr[2]);
        controller.setSndSuccessor(Integer.parseInt(arr[3]));
        System.out.println("My second successor is Peer " + arr[3]);
    }


    private void acceptJoinRequest(int newPeer) {
        System.out.println("Peer " + newPeer + " Join request recieved");

        // new thread started to enure that new tcp requests can be processed
        // needs to wait till first ping is received from 1st predeccessor in order to send them their new successors
        Thread changeSuccessors = new Thread() {
            public void run() {
                while (controller.getFstPredeccessor() == -1);
                int oldFstPred = controller.getFstPredeccessor();
                controller.tcpSender(newPeer, "New Peers: " + controller.getFstSuccessor() + " " + controller.getSndSuccessor());
                updateSuccesssors(newPeer, controller.getFstSuccessor());
                controller.tcpSender(oldFstPred, "Am I your first successor: " + peerID);
            }
        };
        changeSuccessors.start();

        controller.dbg("exiting acceptJoinRequest");
    }

    public void run()  {

        int serverPort = controller.findPort(peerID); 
        ServerSocket welcomeSocket = null;
        try {
            welcomeSocket = new ServerSocket(serverPort);
        } catch (Exception e) {
            System.out.println(e);
        }
        controller.dbg("Server is ready :");
        
        while (true){
            String request = receiveTCPMsg(welcomeSocket);
            // syncLock.unlock();
            processTCPMsg(request);
        }
    }

    /**
     * Receive a TCP msg
     * @param welcomeSocket The socket used for welcoming
     * @return The string received
     */
    private String receiveTCPMsg(ServerSocket welcomeSocket) {

        controller.dbg("TCP: accepting message...");
        String request = null;
        try {
            Socket connectionSocket = welcomeSocket.accept();
        // syncLock.lock();
            BufferedReader inFromClient = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
            request = inFromClient.readLine();
            controller.dbg("TCP message received: " + request);
            connectionSocket.close();
        } catch (Exception e) {
            System.out.println(e);
        }
        return request;
    } 

    // reads the TCP msg and applies the correct function to it
    private void processTCPMsg(String request) {
        String[] arr = request.split(" ");
        if (request.startsWith(controller.JOIN_REQUEST)) 
            processJoinReqMsg(request, arr);
        else if (request.startsWith("New Peers: ")) 
            setSuccessors(arr);
        else if (request.startsWith("Am I your first successor: ")) 
            isFstSuccQueary(arr);
        else if (request.startsWith(SUCCESSOR_CONFIRM)) 
            succConfirmation(arr);
        else if (request.startsWith("New fst Successor: ")) 
            updateSuccesssors(Integer.parseInt(arr[3]), Integer.parseInt(arr[7]));
        else if (request.startsWith(controller.GRACEFUL_DEPARTURE))
            gracefulDeparture(arr);
        else
            System.out.println("Not yet implemented");
    }

    private void gracefulDeparture(String[] arr) {
        int peerNo = Integer.parseInt(arr[15]);
        System.out.println("Peer " + peerNo + " will depart from the network");
        int newFst = Integer.parseInt(arr[10]);
        int newSnd = Integer.parseInt(arr[13]);
        updateSuccesssors(newFst, newSnd);
    }

    // sends new successors to old first predecessor
    private void succConfirmation(String[] arr) {
        int senderID = Integer.parseInt(arr[5]);
        controller.tcpSender(senderID, "New fst Successor: " + peerID + " New snd Successor: " + controller.getFstSuccessor());
    }

    // see if the node joining the network is the rightful successor
    // if so accepts it into the network
    // if not passes msg on to fst successor
    private void processJoinReqMsg(String request, String[] arr) {
        int newPeer = Integer.parseInt(arr[4]);
        if (isRightfulSuccessor(newPeer))
            acceptJoinRequest(newPeer);
        else {
            //TODO skip nodes 
            controller.tcpSender(controller.getFstSuccessor(), request);
            System.out.println("Peer " + newPeer + " Join request forwarded to my successor");
        }
    }

    // checks if sender is its first successor
    // sends back message asking for updated successors
    private void isFstSuccQueary(String[] arr) {
        int senderID = Integer.parseInt(arr[5]);
        if (controller.getFstSuccessor() == senderID) {
            controller.tcpSender(senderID, SUCCESSOR_CONFIRM + peerID);
            System.out.println(CHANGE_REQ_RECEIVED);
        } else
            controller.dbg("predeccessor not found"); // TODO deal with this situation
            // send back ping saying not first successor
    }

    // checks if the joinging node should be this nodes first successor
    private Boolean isRightfulSuccessor(int newPeer) {
        return (newPeer < controller.getFstSuccessor() && newPeer > peerID) || 
               (newPeer > peerID && peerID > controller.getFstSuccessor())  || 
               (peerID > controller.getFstSuccessor() && controller.getFstSuccessor() < newPeer);
    }
}