// Joshua Sumskas - z5208508


import java.io.*;
import java.net.*;
import java.util.*;
import java.text.SimpleDateFormat;
import java.util.concurrent.locks.*;


public class p2p {
    static final boolean DEBUG = true;
    static volatile int fstSuccessor = -1;
    static SocketAddress fstSucSock;
    static int sndSuccessor;
    static SocketAddress sndSucSock;
    static byte[] sendData = new byte[1024];
    static DatagramSocket serverSocket;
    // static int pingInt = 10000;
    static ReentrantLock syncLock = new ReentrantLock();
    static int peer;
    static int predecessor = -1;

    static volatile boolean waiting4predecessor = false;
    static final String ASK_STILL_ALIVE = "U still alive?";
    static final String SUCCESSOR_CHANGE_REQUEST = "Successor Change request from: ";
    static final String JOIN_REQUEST = "Join request from Peer ";
    static final String STILL_ALIVE = "Haven't kicked the bucket yet!";

    public static void main(String[] args) throws Exception {
        String type = args[0];
        peer = Integer.parseInt(args[1]);
        // starting reciever loops

        // final p2p tcpLoop = new p2p();
        Thread tcpLoop = new Thread() {
            public void run() {
                tcpListener();
            }
        };
        tcpLoop.start();

        int pingInt;
        if (type.equals("init")) {

            fstSuccessor = Integer.parseInt(args[2]);
            sndSuccessor = Integer.parseInt(args[3]);
            pingInt = Integer.parseInt(args[4]);
            fstSucSock = new InetSocketAddress("127.0.0.1", findPort(fstSuccessor));
            sndSucSock = new InetSocketAddress("127.0.0.1", findPort(sndSuccessor));

        } else if (type.equals("join")) {

            findSuccessors(Integer.parseInt(args[2]));
            pingInt = Integer.parseInt((args[3]));

        } else {
            System.out.println("Invalid type given. Valide types are: init, join");
            throw new RuntimeException();
        }

        while (fstSuccessor == -1) Thread.sleep(100);
        pingInt *= 1000;

        serverSocket = new DatagramSocket(findPort(peer));

        Thread udpLoop = new Thread() {
            public void run() {
                udpRecieverLoop();
            }
        };
        udpLoop.start();
        pingThread(pingInt);
    }

    private static int findPort(int id) {
        return id + 12000;
    }

    private static void pingS(String msg, SocketAddress addr) {
        sendData = msg.getBytes();
        syncLock.lock();
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, addr);
        try {
            dbg("From: " + peer + ". Sending '" + msg + "' to " + addr.toString());
            serverSocket.send(sendPacket);
            dbg("sent");
        } catch (IOException e){ 
            dbg(msg + " FAILED TO SEND: " + e.getMessage());
        }
        syncLock.unlock();
    }

    private static void findSuccessors(int knownPeer) throws Exception{

        String serverName = "localhost";
        int serverPort = findPort(knownPeer); 

        syncLock.lock();

        Socket clientSocket = new Socket(serverName, serverPort);
        String msg = JOIN_REQUEST + peer;

        DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
        outToServer.writeBytes(msg + '\n');

        clientSocket.close();
        syncLock.unlock();
    }

// Pings successors every x seconds
    private static void pingThread(int pingInt) {
        while(true){
            pingS(ASK_STILL_ALIVE, fstSucSock);
            pingS(ASK_STILL_ALIVE, sndSucSock);          
            System.out.println("Ping requests sent to Peers " + fstSuccessor + " and " + sndSuccessor);
            try {
                Thread.sleep(pingInt);
            } catch (InterruptedException e) {
                System.out.println(e);
            }
        }
    }

    private static void tcpSender(int peerNo, String msg) {

        int serverPort = findPort(peerNo); 
        syncLock.lock();
        try {
            dbg("TCP Sending " + msg + " to " + peerNo);
// System.out.println(peerNo);
            Socket clientSocket = new Socket("localhost", serverPort);
            DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
            outToServer.writeBytes(msg + "\n\r");
            clientSocket.close();
        } catch (Exception e) {
            System.out.println(e);
        }
        syncLock.unlock();

    }

    private static Boolean isRightfulSuccessor(int newPeer) {
        return (newPeer < fstSuccessor && newPeer > peer) || (newPeer > peer && peer > fstSuccessor) || (peer > fstSuccessor && fstSuccessor < newPeer);
    }

    private static void acceptJoinRequest(int newPeer) throws Exception {
        System.out.println("Peer " + newPeer + " Join request recieved");
        tcpSender(newPeer, "New Peers: " + fstSuccessor + " " + sndSuccessor);
        updateSuccesssors(newPeer, fstSuccessor);

        // asks 4 predecessor until a respone is heard
        // requests every 5 seconds until a response if recieved
        Thread looper = new Thread() {
            public void run() {
                waiting4predecessor = true;
                while (waiting4predecessor) {
                    pingS(SUCCESSOR_CHANGE_REQUEST + peer, sndSucSock);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        dbg(e.toString());
                    }
                }
            }
        };
        looper.start();
        dbg("exiting acceptJoinRequest");
    }

    private static void setSuccessors(String[] arr) {
        System.out.print("Join request has been accepted\nMy new first successor is Peer " + arr[2] + "\nMy new second successor is Peer " + arr[3]);
        fstSuccessor = Integer.parseInt(arr[2]);
        sndSuccessor = Integer.parseInt(arr[3]);
        fstSucSock = new InetSocketAddress("127.0.0.1", findPort(fstSuccessor));
        sndSucSock = new InetSocketAddress("127.0.0.1", findPort(sndSuccessor));
    }

    private static void updateSuccesssors(int newFst, int newSnd) {
        fstSuccessor = newFst;
        sndSuccessor = newSnd;
        fstSucSock = new InetSocketAddress("127.0.0.1", findPort(fstSuccessor));
        sndSucSock = new InetSocketAddress("127.0.0.1", findPort(sndSuccessor)); 
        System.out.println("My new first successor is Peer " + fstSuccessor);
        System.out.println("My new second successor is Peer " + sndSuccessor);
    }

    private static void tcpListener()  {

        int serverPort = findPort(peer); 
        ServerSocket welcomeSocket = null;
        try {
            welcomeSocket = new ServerSocket(serverPort);
        } catch (Exception e) {
            System.out.println(e);
        }
        dbg("Server is ready :");
        
        while (true){
            try {
                dbg("TCP: accepting message...");
                Socket connectionSocket = welcomeSocket.accept();
                syncLock.lock();
                BufferedReader inFromClient = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
                String request;
                request = inFromClient.readLine();
                String[] arr = request.split(" ");
                dbg("TCP message received: " + request);

                connectionSocket.close();
                syncLock.unlock();

                if (request.startsWith(JOIN_REQUEST)) {
                    int newPeer = Integer.parseInt(arr[4]);
                    if (isRightfulSuccessor(newPeer)) {
                        acceptJoinRequest(newPeer);
                    } else {
                        if ((newPeer < sndSuccessor && newPeer > fstSuccessor) || (newPeer > fstSuccessor && fstSuccessor > sndSuccessor) || (fstSuccessor > sndSuccessor && sndSuccessor < newPeer)) {
                        // newPeer > fstSuccessor && newPeer < sndSuccessor
                            sndSuccessor = newPeer;
                            sndSucSock = new InetSocketAddress("127.0.0.1", findPort(newPeer));  
                        }
                        tcpSender(fstSuccessor, request);
                        System.out.println("Peer " + newPeer + " Join request forwarded to my successor");
                    }
                } else if (request.startsWith("New Peers: ")) {
                    setSuccessors(arr);
                } else if (request.startsWith("Requesting Successors ")) {

                    dbg("sending to: " + arr[2]);
                    tcpSender(Integer.parseInt(arr[2]), "New fst Successor: " + peer + " New snd Successor: " + fstSuccessor);
                    waiting4predecessor = false;
                } else if (request.startsWith("New fst Successor: ")) {
                    
                    updateSuccesssors(Integer.parseInt(arr[3]), Integer.parseInt(arr[7]));
                } else {
System.out.println("Not yet implemented");
                } 

            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }

    private static void udpRecieverLoop() {
        String sentence = null;
//         //prepare buffers
        byte[] receiveData = null;
        SocketAddress sAddr;

        while (true) {
            //receive UDP datagram
            receiveData = new byte[1024];
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            try {
                serverSocket.receive(receivePacket);
            } catch (Exception e) {
                System.out.println(e);
            }
            //get data
            sentence = new String(receivePacket.getData());
            //Need only the data received not the spaces till size of buffer
            sentence = sentence.trim();
            dbg("received: " + sentence);
            
            syncLock.lock();
            try {
                sAddr = receivePacket.getSocketAddress();
            }
            finally { 
                syncLock.unlock();
            }
            String[] arr = sentence.split(" ");
            if(sentence.equals(ASK_STILL_ALIVE)) {
                int senderId = receivePacket.getPort();
                
                pingS(STILL_ALIVE, sAddr);
                System.out.println("Ping request message recieved from Peer " + (senderId - 12000));
            }
            else if (sentence.equals(STILL_ALIVE)) {
                int senderId = receivePacket.getPort();
                System.out.println("Ping response received from Peer " + (senderId - 12000)); 
                // deal with abrupt departure
            } 
            else if (sentence.startsWith(SUCCESSOR_CHANGE_REQUEST)) {

                int peerAsking = Integer.parseInt(arr[4]);
                if (fstSuccessor == peerAsking) {
                    // System.out.println("Successor Change request received");
                    dbg("Successor Change request received");
                    tcpSender(peerAsking, "Requesting Successors " + peer);
                } else {
                    pingS(sentence, fstSucSock);
                }
            }

        } // end of while (true)
    }
    static void dbg(String str) {
        if (DEBUG) {
            System.out.println("P" + peer + ", " + str);
        }
    }
    
}