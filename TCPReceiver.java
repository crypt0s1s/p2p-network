
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
    final String SUCCESSOR_QUEARY = "Am I your first successor: "; 

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
        controller.fstSuccessorMissedPingsReset();
        controller.setSndSuccessor(newSnd);
        System.out.println("My new second successor is Peer " + newSnd);
        controller.sndSuccessorMissedPingsReset();
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

    /**
     * Called when the new peer is the rightful first successor of the current peer.
     * Sends new peer details of successors
     * @param newPeer The peer asking to join the network
     */
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
            receiveTCPMsg(welcomeSocket);
        }
    }

    /**
     * Receive a TCP msg
     * @param welcomeSocket The socket used for welcoming
     * @return The string received
     */
    private void receiveTCPMsg(ServerSocket welcomeSocket) {

        controller.dbg("TCP: accepting message...");
        String request = null;
        try {
            Socket connectionSocket = welcomeSocket.accept();
        // syncLock.lock();
            BufferedReader inFromClient = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
            request = inFromClient.readLine();
            processTCPMsg(request, connectionSocket);
            controller.dbg("TCP message received: " + request);
            connectionSocket.close();
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            // syncLock.unlock();
        }
    } 

    /**
     * Processes the request and applies the correct function to it
     * @param request The msg received
     */
    private void processTCPMsg(String request, Socket connectionSocket) {
        String[] arr = request.split(" ");
        if (request.startsWith(controller.JOIN_REQUEST)) 
            processJoinReqMsg(request, arr);
        else if (request.startsWith("New Peers: ")) 
            setSuccessors(arr);
        else if (request.startsWith(SUCCESSOR_QUEARY)) 
            isFstSuccQueary(arr);
        else if (request.startsWith(SUCCESSOR_CONFIRM)) 
            succConfirmation(arr);
        else if (request.startsWith(controller.NEW_FST_SUCCESSOR)) 
            updateSuccesssors(Integer.parseInt(arr[3]), Integer.parseInt(arr[7]));
        else if (request.startsWith(controller.GRACEFUL_DEPARTURE))
            gracefulDeparture(arr);
        else if (request.startsWith(controller.DEAD_NODE_DETECTED))
            abruptDeparture(arr);
        else if (request.startsWith(controller.STORE_REQ))
            controller.storeRequest(arr[4]);
        else if (request.startsWith(controller.FILE_REQ))
            fileRequest(request, arr);
        else if (request.startsWith(controller.SENDING_FILE_NOTICE))
            startFileReceive(arr, connectionSocket);
        else
            System.out.println("Not yet implemented: " + request);
    }

    private void startFileReceive(String[] arr, Socket connectionSocket) {
        String file = arr[5];
        String fileType = arr[8];
        String sender = arr[11];
        String length = arr[12];
        String newFileName = "received_" + file + "." + fileType;
        System.out.println("Peer " + sender + " had File " + file);
        System.out.println("Receiving File " + file + " from Peer " + sender);

        receiveFile(newFileName, connectionSocket, length);

        System.out.println("File " + file + " received");
    }

    // https://www.rgagnon.com/javadetails/java-0542.html

    private void receiveFile(String newFileName, Socket connectionSocket, String length) {

                   // reading the file name from keyboard. Uses input stream

                // sending the file name to server. Uses PrintWriter         
                            // receiving the contents from server.  Uses input stream

        System.out.println(newFileName);
        BufferedOutputStream bos = null;
        int l = Integer.parseInt(length);
        if (l <= 0) l = 1024;

        try {
            byte[] mybytearray = new byte[l];
            InputStream is = connectionSocket.getInputStream();
            FileOutputStream fos = new FileOutputStream(newFileName);
            bos = new BufferedOutputStream(fos);
            int bytesRead = is.read(mybytearray, 0, mybytearray.length);
            bos.write(mybytearray, 0, bytesRead);
            bos.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (bos != null) {
                try {
                    bos.close();
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
        }

        

        // long l = Long.parseLong(length);
        // System.out.println("hello");
        // int fileSize = (int) l;
        // System.out.println(fileSize);
        // File newFile = new File(newFileName);
        
        // int bytesRead;
        // int current = 0;
        // FileOutputStream fos = null;
        // BufferedOutputStream bos = null;

        // try {
        //     // receive file
        //     byte [] mybytearray  = new byte [fileSize];
        //     InputStream is = connectionSocket.getInputStream();
        //     fos = new FileOutputStream(newFile);

        //     bos = new BufferedOutputStream(fos);
        //     bytesRead = is.read(mybytearray, 0, mybytearray.length);
        //     current = bytesRead;

        //     do {
        //         bytesRead = is.read(mybytearray, current, (mybytearray.length - current));
        //        if (bytesRead >= 0) current += bytesRead;
        //     } while (bytesRead > -1);
        //     System.out.println("marker E: " + current);
        //     current = 63;
        //     bos.write(mybytearray, 0 , current);
        //     System.out.println("marker A: " + current);
        //     bos.flush();
        //     System.out.println("marker B");
        //     // System.out.println("File " + FILE_TO_RECEIVED
        //     //     + " downloaded (" + current + " bytes read)");
        // } catch (Exception e) {
        //     System.out.println(e);
        // }
        // finally {
        //     if (fos != null) try {
        //         fos.close();
        //     } catch (Exception e) {
        //         System.out.println(e);
        //     }
        //     if (bos != null) try {
        //         bos.close();
        //     } catch (Exception e) {
        //         System.out.println(e);
        //     } 
        // }
        // Socket clientSocket = new Socket("localhost", serverPort);
        // DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
        // outToServer.writeBytes(msg + '\n');
        // DataInputStream inFromFile = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
        // DataOutputStream outToFile = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(newFile)));
        // BufferedReader inFromClient = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
        // outToFile.write(inFromClient.lines());
    }


    private void fileRequest(String msg, String[] arr) {
        String file = arr[2];
        int fileNo = Integer.parseInt(file);
        int hashedVal = Math.floorMod(fileNo, 256);
        int receipient = -1;
        if (controller.shouldFileBeStoredHere(hashedVal)) {
            controller.findAndSendFile(file, Integer.parseInt(arr[6]));
        } else {
            if (controller.isPeerRightfulStorer(controller.getPeerId(), controller.getFstSuccessor(), hashedVal))
                receipient = controller.getFstSuccessor();
            else
                receipient = controller.getSndSuccessor();
            controller.tcpSender(receipient, msg);
            System.out.println("Request for File " + file + " has been received, but the file is not stored here");
        }
    }

    /**
     * Handles abrubt departure process
     * @param arr The arguments of the message received
     */
    private void abruptDeparture(String[] arr) {
        int deadNode = Integer.parseInt(arr[3]);
        int senderID = Integer.parseInt(arr[5]);
        if (deadNode == controller.getFstSuccessor()) {
            controller.tcpSender(senderID, controller.NEW_FST_SUCCESSOR + peerID + controller.NEW_SND_SUCCESSOR + controller.getSndSuccessor());
        } else {
            controller.tcpSender(senderID, controller.NEW_FST_SUCCESSOR + peerID + controller.NEW_SND_SUCCESSOR + controller.getFstSuccessor()); 
        }
    }

    /**
     * Handles graceful departure process
     * @param arr The arguments of the message recieved
     */
    private void gracefulDeparture(String[] arr) {
        int peerNo = Integer.parseInt(arr[15]);
        System.out.println("Peer " + peerNo + " will depart from the network");
        int newFst = Integer.parseInt(arr[10]);
        int newSnd = Integer.parseInt(arr[13]);
        updateSuccesssors(newFst, newSnd);
    }

    /**
     * Sends new successors to old first predecessor
     * @param arr The arguments of the message recieved
     */
    private void succConfirmation(String[] arr) {
        int senderID = Integer.parseInt(arr[5]);
        controller.tcpSender(senderID, controller.NEW_FST_SUCCESSOR + peerID + controller.NEW_SND_SUCCESSOR + controller.getFstSuccessor());
    }

    //
    /**
     * See if the node joining the network is the rightful successor
     * If so accepts it into the network
     * If not passes msg on to fst successor
     * @param request The request received
     * @param arr The arguments of the message received
     */
    private void processJoinReqMsg(String request, String[] arr) {
        int newPeer = Integer.parseInt(arr[4]);
        if (isRightfulSuccessor(newPeer, peerID, controller.getFstSuccessor()))
            acceptJoinRequest(newPeer);
        else {
            int forwardingTo = -1;
            if (isRightfulSuccessor(newPeer, controller.getFstSuccessor(), controller.getSndSuccessor()))
                forwardingTo = controller.getFstSuccessor();
            else
                forwardingTo = controller.getSndSuccessor();
            controller.tcpSender(forwardingTo, request);
            System.out.println("Peer " + newPeer + " Join request forwarded to my successor");
        }
    }

    /**
     * Checks if sender is its first successor
     * If so sends back message asking for updated successors
     * @param arr The arguments of the message received
     */
    private void isFstSuccQueary(String[] arr) {
        int senderID = Integer.parseInt(arr[5]);
        if (controller.getFstSuccessor() == senderID) {
            controller.tcpSender(senderID, SUCCESSOR_CONFIRM + peerID);
            System.out.println(CHANGE_REQ_RECEIVED);
        } else {
            // Thread changeSuccessors = new Thread() {
            //     public void run() {

            //         //slee[]
            //         while (controller.getFstPredeccessor() == -1);
            //         controller.tcpSender(newPeer, "New Peers: " + controller.getFstSuccessor() + " " + controller.getSndSuccessor());
            //         updateSuccesssors(newPeer, controller.getFstSuccessor());
            //         int oldFstPred = controller.getFstPredeccessor();
            //         controller.tcpSender(oldFstPred, "Am I your first successor: " + peerID);
            // };
            //     }
            // changeSuccessors.start();
        }
            // send back ping saying not first successor
    }

    /**
     * Checks if the joinging node should be the specified nodes first successor
     * @param newPeer The peer joining the network
     * @param peer The peer that is being questioned
     * @param successor The successor of the peer that is being questioned
     * @return If the joinging node should be this nodes first successor
     */
    private Boolean isRightfulSuccessor(int newPeer, int peer, int successor) {
        return (newPeer < successor && newPeer > peer) || 
               (newPeer > peer && peer > successor)  || 
               (peer > successor && successor < newPeer);
    }
}