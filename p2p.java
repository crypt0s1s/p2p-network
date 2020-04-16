// Joshua Sumskas - z5208508

import java.io.*;
import java.net.*;
import java.util.*;
// import java.util.*;
import java.util.concurrent.locks.*;


public class p2p implements Runnable {

    final boolean DEBUG = false;

    static String[] initArgs;

    int peerID;
    
    volatile int fstSuccessor = -1;
    volatile SocketAddress fstSucSock;
    volatile int fstSuccessorMissedPings = 0;
    volatile int sndSuccessor;
    volatile SocketAddress sndSucSock;
    volatile int sndSuccessorMissedPings = 0;
    volatile int fstPredeccessor = -1;
    volatile int sndPredeccessor = -1;

    int pingInt;

    byte[] sendData = new byte[1024];
    DatagramSocket serverSocket;
    ReentrantLock syncLock = new ReentrantLock();

    ArrayList<Integer> storedFiles = new ArrayList<Integer>();

    final String ASK_STILL_ALIVE = "U still alive?";
    final String SUCCESSOR_CHANGE_REQUEST = "Successor Change request from: ";
    final String JOIN_REQUEST = "Join request from Peer ";
    final String STILL_ALIVE = "Haven't kicked the bucket yet!";
    final String GRACEFUL_DEPARTURE = "Though shall no longer be in thees presence.";
    final String DEAD_NODE_DETECTED = "Dead node detected: ";
    final String NEW_FST_SUCCESSOR = "New first successor: ";
    final String NEW_SND_SUCCESSOR = " New second successor: ";
    final String STORE_REQ = "Store request for file: ";

    public static void main(String[] args) throws Exception {
        initArgs = args;
        new Thread(new p2p()).start();
    }

    public int findPort(int id) {
        return id + 12000;
    }

    /**
     * Sends a message to a specified peer over UDP.
     * @param msg The message to send to the peer.
     * @param addr The socket of the specified peer.
     */
    public void pingS(String msg, SocketAddress addr) {
        sendData = msg.getBytes();
        syncLock.lock();
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, addr);
        try {
            dbg("From: " + peerID + ". Sending '" + msg + "' to " + addr.toString());
            serverSocket.send(sendPacket);
            dbg("sent");
        } catch (IOException e){ 
            dbg(msg + " FAILED TO SEND: " + e.getMessage());
        }
        syncLock.unlock();
    }

 
    /**
     * Sends a message to a specified peer over TCP.
     * @param peerNo Peer the message needs to be send to.
     * @param msg The message to send to the peer.
     */
    public void tcpSender(int peerNo, String msg) {

        int serverPort = findPort(peerNo); 
        syncLock.lock();
        try {
            dbg("TCP Sending " + msg + " to " + peerNo);
            Socket clientSocket = new Socket("localhost", serverPort);
            DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
            outToServer.writeBytes(msg + '\n');
            clientSocket.close();
        } catch (Exception e) {
            System.out.println(e);
        }
        syncLock.unlock();

    }

    public void run() {
        peerID = Integer.parseInt(initArgs[1]);
        
        // creates tcp listening thread
        new Thread(new TCPReceiver(peerID, this)).start();
        
        // set up initial parameters
        nodeSetup();

        // creates udp listenting thread and udp pingging thread
        new Thread(new UDPReceiver(peerID, this, serverSocket)).start();
        new Thread(new UDPPinger(pingInt, this)).start();

        commandThread();
    }

    /**
     * Reads and processes commands
     */
    private void commandThread() {
        Scanner scanner = new Scanner(System.in);
        while(true) {
            String msg = scanner.nextLine();
            processCommand(msg);
        }
    }

    /**
     * Process the stdin commands
     * @param msg The stdin command
     */
    private void processCommand(String msg) {
        if (msg.equals("Quit"))
            handleQuit();
        else if (msg.startsWith("Store ") && msg.length() == 10) {//TODO add more safety on this section
            String[] arr = msg.split(" ");
            storeRequest(arr[1]);
        } else if (msg.startsWith("Request ") && msg.length() == 12); //TODO
        else if (msg.equals("Show Stored Files"))
            showStoredFiles();
        else System.out.println("Unknown request");
    }

    /**
     * Prints the stored files file numbers
     */
    private void showStoredFiles() {
        Iterator<Integer> i = storedFiles.iterator();
        System.out.println("The Files stored at this node are:");
        while (i.hasNext()) {
           System.out.println(i.next());
        }
    }

    /**
     * Handles a store request Decides if file belongs to this peer or should be
     * forwarded to another peer
     * 
     * @param file The file to be stored
     */
    public void storeRequest(String file) {
        int fileNo = Integer.parseInt(file);
        int hashedVal = Math.floorMod(fileNo, 256);

        if (shouldFileBeStoredHere(hashedVal)) {
            storedFiles.add(fileNo);
            System.out.println("Store " + fileNo + " reuqest accepted");
        } else {
            forwardStoreReq(fileNo);
        }
    }

    /**
     * Checks if the files should be stored at this peer
     * If predeccessor is not found yet waits till predecessor is known
     * @param hashedVal The hashed value of the file
     * @return If the file should be stored at this peer
     */
    private boolean shouldFileBeStoredHere(int hashedVal) {
        while(fstPredeccessor == -1);
        return isPeerRightfulStorer(fstPredeccessor, peerID, hashedVal);
    }

    /**
     * Forwards a file store request
     * If it belongs to the first successor send it
     * to the first successor 
     * Else send it to the second successor
     * @param fileNo The file to be stored
     */
    private void forwardStoreReq(int fileNo) {
        int hashedVal = Math.floorMod(fileNo, 256);
        int forwardee;
        if (isPeerRightfulStorer(peerID, fstSuccessor, hashedVal))
            forwardee = fstSuccessor;
        else
            forwardee = sndSuccessor;
        tcpSender(forwardee, STORE_REQ + fileNo);
        System.out.println("Store " + fileNo + " request forwarded to my successor");
    }

    /**
     * Handles Quit command Sends details to predeccessors about new successors
     */
    private void handleQuit() {
        while(fstPredeccessor == -1);
        tcpSender(fstPredeccessor, GRACEFUL_DEPARTURE + " New fst: " + fstSuccessor + " New snd: " + sndSuccessor + " From: " + peerID);
        while(sndPredeccessor == -1);
        tcpSender(sndPredeccessor, GRACEFUL_DEPARTURE + " New fst: " + fstPredeccessor + " New snd: " + fstSuccessor + " From: " + peerID);
        System.exit(0);
    }

    private boolean isPeerRightfulStorer(int peer, int peersFstSucc, int hashedVal) {
        return 
            (peer < hashedVal && hashedVal <= peersFstSucc) || 
            (peersFstSucc < peer && peer < hashedVal) || 
            (hashedVal <= peersFstSucc && peersFstSucc < peer);
    }

    /**
     * Handles init start command
     */
    private void initialiseNode() {
        setFstSuccessor(Integer.parseInt(initArgs[2]));
        setSndSuccessor(Integer.parseInt(initArgs[3]));
        pingInt = Integer.parseInt(initArgs[4]) * 1000;
    }

    /**
     * Handles join start command
     */
    private void initialiseJoin() {
        int knownPeer = Integer.parseInt(initArgs[2]); 
        int serverPort = findPort(knownPeer); 

        syncLock.lock();
        try {
            sendJoinRequest(serverPort);
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            syncLock.unlock();
        }    
        pingInt = Integer.parseInt((initArgs[3])) * 1000;
    }

    /**
     * Sends the join request msg to the known peer
     * @param serverPort The port of the known peer
     * @throws Exception
     */
    private void sendJoinRequest(int serverPort) throws Exception {
        String serverName = "localhost";
        Socket clientSocket = new Socket(serverName, serverPort);
        String msg = JOIN_REQUEST + peerID;

        DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
        outToServer.writeBytes(msg + '\n');
        clientSocket.close();
    }

    /**
     * Sets up node based on type
     */
    private void nodeSetup() {
        nodeTypeSetup(initArgs[0]);
        generalNodeSetup();
    }

    /**
     * Sets up general node procedures
     * While loop is so that join requests can be proccessed before further actions are taken
     */
    private void generalNodeSetup() {
        while (fstSuccessor == -1) {
            try {
                Thread.sleep(100);    
            } catch (InterruptedException e) {
               System.out.println(e);
            }
        }
        try {
            serverSocket = new DatagramSocket(findPort(peerID));
        } catch (Exception e) {
            System.out.println("Error creating socket");
            System.exit(0);
        }
    }

    /**
     * Sets up node based on type
     * @param type The specified initial type of the node
     */
    private void nodeTypeSetup(String type) {
        if (type.equals("init"))
            initialiseNode();
        else if (type.equals("join"))
            initialiseJoin();
        else {
            System.out.println("Invalid type given. Valide types are: init, join");
            throw new RuntimeException();
        }
    }

    void dbg(String str) {
        if (DEBUG) {
            System.out.println("P" + peerID + ", " + str);
        }
    }

    public SocketAddress getFstSuccSocket() {
        return fstSucSock;
    } 

    public SocketAddress getSndSuccSocket() {
        return sndSucSock;
    } 

    public int getFstSuccessor() {
        return fstSuccessor;
    }

    public void setFstSuccessor(int newFstSuccessor) {
        fstSuccessor = newFstSuccessor;
        fstSucSock = new InetSocketAddress("127.0.0.1", findPort(fstSuccessor));
    }
    
    public int getSndSuccessor() {
        return sndSuccessor;
    }

    public void setSndSuccessor(int newSndSuccessor) {
        sndSuccessor = newSndSuccessor;
        sndSucSock = new InetSocketAddress("127.0.0.1", findPort(sndSuccessor));
    }
    
    public int getFstPredeccessor() {
        return fstPredeccessor;
    }

    public void setFstPredeccessor(int newFstPredeccessor) {
        fstPredeccessor = newFstPredeccessor;        
    }

    public int getSndPredeccessor() {
        return sndPredeccessor;
    }

    public void setSndPredeccessor(int newSndPredeccessor) {
        sndPredeccessor = newSndPredeccessor;     
    }

    public void fstSuccessorMissedPingsIncrement() {
        fstSuccessorMissedPings++;
    }

    public void fstSuccessorMissedPingsReset() {
        fstSuccessorMissedPings = 0;
    }

    public int fstSuccessorMissedPingCount() {
        return fstSuccessorMissedPings;
    }

    public void sndSuccessorMissedPingsIncrement() {
        sndSuccessorMissedPings++;
    }

    public void sndSuccessorMissedPingsReset() {
        sndSuccessorMissedPings = 0;
    }

    public int sndSuccessorMissedPingCount() {
        return sndSuccessorMissedPings;
    }

    public int getPeerId() {
        return peerID;
    }
}