// Joshua Sumskas - z5208508

import java.io.*;
import java.net.*;
import java.util.*;
// import java.util.*;
import java.util.concurrent.locks.*;


public class p2p implements Runnable {

    static final boolean DEBUG = false;
    static volatile int fstSuccessor = -1;
    static SocketAddress fstSucSock;
    static int fstSuccessorMissedPings = 0;
    static int sndSuccessor;
    static SocketAddress sndSucSock;
    static int sndSuccessorMissedPings = 0;

    static byte[] sendData = new byte[1024];
    static DatagramSocket serverSocket;
    // static int pingInt = 10000;
    static ReentrantLock syncLock = new ReentrantLock();
    static int peerID;
    static volatile int fstPredeccessor = -1;
    static volatile int sndPredeccessor = -1;
    static String[] initArgs;
    static int pingInt;



    final String ASK_STILL_ALIVE = "U still alive?";
    final String SUCCESSOR_CHANGE_REQUEST = "Successor Change request from: ";
    final String JOIN_REQUEST = "Join request from Peer ";
    final String STILL_ALIVE = "Haven't kicked the bucket yet!";
    final String GRACEFUL_DEPARTURE = "Though shall no longer be in thees presence.";

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
            outToServer.writeBytes(msg + "\n\r");
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
            // wait for command
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
    }

    /**
     * Handles Quit command
     * Sends details to predeccessors about new successors
     */
    private void handleQuit() {
        while(fstPredeccessor == -1);
        tcpSender(fstPredeccessor, GRACEFUL_DEPARTURE + " New fst: " + fstSuccessor + " New snd: " + sndSuccessor + " From: " + peerID);
        while(sndPredeccessor == -1);
        tcpSender(sndPredeccessor, GRACEFUL_DEPARTURE + " New fst: " + fstPredeccessor + " New snd: " + fstSuccessor + " From: " + peerID);
        System.exit(0);
    }

    /**
     * Handles init start command
     */
    private void initialiseNode() {
        setFstSuccessor(Integer.parseInt(initArgs[2]));
        setSndSuccessor(Integer.parseInt(initArgs[3]));
        pingInt = Integer.parseInt(initArgs[4]);
    }

    /**
     * Handles join start command
     */
    private void initialiseJoin() {
        int knownPeer = Integer.parseInt(initArgs[2]); 
        String serverName = "localhost";
        int serverPort = findPort(knownPeer); 

        syncLock.lock();
        try {
            Socket clientSocket = new Socket(serverName, serverPort);
            String msg = JOIN_REQUEST + peerID;
    
            DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
            outToServer.writeBytes(msg + '\n');
            clientSocket.close();
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            syncLock.unlock();
        }    
        pingInt = Integer.parseInt((initArgs[3]));

    }

    /**
     * Sets up node based on type
     */
    private void nodeSetup() {
        String type = initArgs[0];
        if (type.equals("init"))
            initialiseNode();
        else if (type.equals("join"))
            initialiseJoin();
        else {
            System.out.println("Invalid type given. Valide types are: init, join");
            throw new RuntimeException();
        }

        while (fstSuccessor == -1) {
            try {
                Thread.sleep(100);    
            } catch (InterruptedException e) {
               System.out.println(e);
            }
            
        }
        pingInt *= 1000;
        try {
            serverSocket = new DatagramSocket(findPort(peerID));
        } catch (Exception e) {
            System.out.println("Error creating socket");
            System.exit(0);
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

    public void sndSuccessorMissedPingsIncrement() {
        sndSuccessorMissedPings++;
    }

    public void sndSuccessorMissedPingsReset() {
        sndSuccessorMissedPings = 0;
    }
}