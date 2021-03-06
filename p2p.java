// Joshua Sumskas - z5208508
// Recomended ping interval is 15 seconds

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.locks.*;
import java.util.regex.Pattern;


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

    ArrayList<String> storedFiles = new ArrayList<String>();

    final String ASK_STILL_ALIVE = "U still alive?";
    final String SUCCESSOR_CHANGE_REQUEST = "Successor Change request from: ";
    final String JOIN_REQUEST = "Join request from Peer ";
    final String STILL_ALIVE = "Haven't kicked the bucket yet!";
    final String GRACEFUL_DEPARTURE = "Though shall no longer be in thees presence.";
    final String DEAD_NODE_DETECTED = "Dead node detected: ";
    final String NEW_FST_SUCCESSOR = "New first successor: ";
    final String NEW_SND_SUCCESSOR = " New second successor: ";
    final String STORE_REQ = "Store request for file: ";
    final String FILE_REQ = "Requesting file ";
    final String SENDING_FILE_NOTICE = "Fullfilling file request. Sending file ";
    final String INVALID_COMMAND = "The command that was just entered is invaid";

    public static void main(String[] args) throws Exception {
        initArgs = args;
        new Thread(new p2p()).start();
    }

    /**
     * Finds the port number of a given ID
     * 
     * @param id
     * @return
     */
    public int findPort(int id) {
        return id + 12000;
    }

    /**
     * Sends a message to a specified peer over UDP.
     * 
     * @param msg  The message to send to the peer.
     * @param addr The socket of the specified peer.
     */
    public void pingS(String msg, SocketAddress addr) {
        sendData = msg.getBytes();
        syncLock.lock();
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, addr);
        try {
            serverSocket.send(sendPacket);
        } catch (IOException e) {
            e.printStackTrace();
        }
        syncLock.unlock();
    }

    /**
     * Sends a message to a specified peer over TCP.
     * 
     * @param peerNo Peer the message needs to be send to.
     * @param msg    The message to send to the peer.
     */
    public void tcpSender(int peerNo, String msg) {

        int serverPort = findPort(peerNo);
        syncLock.lock();
        try {
            Socket clientSocket = new Socket("localhost", serverPort);
            DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
            outToServer.writeBytes(msg + '\n');
            clientSocket.close();
        } catch (Exception e) {
            e.printStackTrace();
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
        while (true) {
            String msg = scanner.nextLine();
            processCommand(msg);
        }
    }

    /**
     * Process the stdin commands
     * 
     * @param msg The stdin command
     */
    private void processCommand(String msg) {
        if (msg.equals("Quit"))
            handleQuit();
        else if (msg.startsWith("Store ")) {
            String[] arr = msg.split(" ");
            if (arr.length != 2) System.out.println(INVALID_COMMAND);
            storeRequest(arr[1]);
        } else if (msg.startsWith("Request "))
            retrieveFile(msg);
        else if (msg.equals("Show Stored Files"))
            showStoredFiles();
        else if (msg.length() == 0);
        else 
            System.out.println(INVALID_COMMAND);
    }

    /**
     * Finds all files with requested name and sends them indiviually
     * @param file The name of the requested file
     * @param peerRequesting The peer requesting the file
     */
    public void findAndSendFile(String file, int peerRequesting) {
        if (storedFiles.contains(file)) {
            System.out.println("File " + file + " is stored here");
            File projectFolder = new File(".");
            boolean fileFound = false;
            File[] files = projectFolder.listFiles();
            for (File f : files) {
                if (f.getName().startsWith(file + ".")) {
                    prepareAndSendFile(f, file, peerRequesting);
                    fileFound = true;
                }
            }
            if (!fileFound) System.out.println("File is said to be stored at this node but no file was found.");
        } else {
            System.out.println("File is not stored at this node despite it being the correct location for the file.");
        }
    }

    /**
     * Handles printing to terminal and basic functions to gather information needed 
     * in order to send the file.
     * @param requestedFile The file that is requested
     * @param file The name of the requested file
     * @param peerRequesting The peer that requests the file
     */
    private void prepareAndSendFile(File requestedFile, String file, int peerRequesting) {

        System.out.println("Sending file " + file + " to Peer " + peerRequesting);
        String fullName = requestedFile.getName();
        String fileType = fullName.split("[.]")[1];
        String msg = SENDING_FILE_NOTICE + file + " of type: " + fileType + " from Peer: " + peerID + " " + requestedFile.length();
        sendFile(msg, peerRequesting, requestedFile);
        System.out.println("The file has been sent");
    }

    /**
     * Sends specified file to requesting peer
     * Codes is based from http://www.java2s.com/Code/Java/Network-Protocol/TransferafileviaSocket.htm
     * @param msg The initial message detailing what is being sent
     * @param peerRequesting The peer requeting the file
     * @param file The file being sent
     */
    private void sendFile(String msg, int peerRequesting, File file) {

        int serverPort = findPort(peerRequesting);

        BufferedInputStream bis = null;
        OutputStream os = null;
        Socket clientSocket = null;

        syncLock.lock();
        try {
            clientSocket = new Socket("localhost", serverPort);
            DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
            outToServer.writeBytes(msg + '\n');

            byte[] mybytearray = new byte[(int) file.length()];
            bis = new BufferedInputStream(new FileInputStream(file));
            bis.read(mybytearray, 0, mybytearray.length);
            os = clientSocket.getOutputStream();
            os.write(mybytearray, 0, mybytearray.length);
            os.flush();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bis != null)
                try {
                    bis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            if (os != null)
                try {
                    os.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            if (clientSocket != null)
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
        syncLock.unlock();
    }

    /**
     * Called when a file request is made
     * If file is stored at this peer user is called a numpty
     * Otherwise sends message successors asking for the file
     * @param msg The command entered
     */
    private void retrieveFile(String msg) {
        String[] arr = msg.split(" ");
        String file = arr[1];
        if (arr.length != 2 || file.length() != 4 || !Pattern.matches("[0-9]*", file)) {
            System.out.println(INVALID_COMMAND);
            return;
        }
        int fileNo = Integer.parseInt(file);
        int hashedVal = Math.floorMod(fileNo, 256);
        int receipient = -1;
        if (shouldFileBeStoredHere(hashedVal))
            System.out.println("The file is already stored by this peer ya numpty"); // <3
        else {
            if (isPeerRightfulStorer(peerID, fstSuccessor, hashedVal))
                receipient = fstSuccessor;
            else
                receipient = sndSuccessor;
            tcpSender(receipient, FILE_REQ + file + " Request from peer " + peerID);
            System.out.println("File request for " + file + " has been sent to my successor");
        }
    }

    /**
     * Prints the stored files file numbers
     */
    private void showStoredFiles() {
        Iterator<String> i = storedFiles.iterator();
        System.out.println("The Files stored at this node are:");
        while (i.hasNext()) {
           System.out.println(i.next());
        }
    }

    /**
     * Handles a store request Decides if file belongs to this peer or should be
     * forwarded to another peer
     * Checks if file name is valid 
     * @param file The file to be stored
     */
    public void storeRequest(String file) {
        if (file.length() != 4 || !Pattern.matches("[0-9]*", file)) {
            System.out.println(INVALID_COMMAND);
            return;
        }
        int fileNo = Integer.parseInt(file);
        int hashedVal = Math.floorMod(fileNo, 256);

        if (shouldFileBeStoredHere(hashedVal)) {
            storedFiles.add(file);
            System.out.println("Store " + file + " request accepted");
        } else {
            forwardStoreReq(file, hashedVal);
        }
    }

    /**
     * Checks if the files should be stored at this peer
     * If predeccessor is not found yet waits till predecessor is known
     * @param hashedVal The hashed value of the file
     * @return If the file should be stored at this peer
     */
    public boolean shouldFileBeStoredHere(int hashedVal) {
        while(fstPredeccessor == -1) {
            try {
                Thread.sleep(100);
            } catch (Exception e) {
                //TODO: handle exception
            }
        }
        return isPeerRightfulStorer(fstPredeccessor, peerID, hashedVal);
    }

    /**
     * Forwards a file store request
     * If it belongs to the first successor send it
     * to the first successor 
     * Else send it to the second successor
     * @param fileNo The file to be stored
     */
    private void forwardStoreReq(String file, int hashedVal) {
        int forwardee;
        if (isPeerRightfulStorer(peerID, fstSuccessor, hashedVal))
            forwardee = fstSuccessor;
        else
            forwardee = sndSuccessor;
        tcpSender(forwardee, STORE_REQ + file);
        System.out.println("Store " + file + " request forwarded to my successor");
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

    /**
     * Checks if the given peers first successor is the correct place to store a file
     * @param peerFstPredeccessor The given peer's fst predeccessor
     * @param peer The  given peer
     * @param hashedVal The hashed value of the file
     * @return If the peer is the correct storer
     */
    public boolean isPeerRightfulStorer(int peerFstPredeccessor, int peer, int hashedVal) {
        return 
            (peerFstPredeccessor < hashedVal && hashedVal <= peer) || 
            (peer < peer && peerFstPredeccessor < hashedVal) || 
            (hashedVal <= peer && peer < peerFstPredeccessor);
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
            e.printStackTrace();
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