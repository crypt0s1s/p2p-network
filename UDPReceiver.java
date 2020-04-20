
import java.io.*;
import java.net.*;
import java.util.*;
import java.text.SimpleDateFormat;
import java.util.concurrent.locks.*;

public class UDPReceiver implements Runnable {

    int peerID;
    p2p controller;
    DatagramSocket serverSocket;

    public UDPReceiver(int peerID, p2p controller, DatagramSocket serverSocket) {
        this.peerID = peerID;
        this.controller = controller;
        this.serverSocket = serverSocket;

    }

    /**
     * Starts a UDP receiving loop.
     * When a message is received processes message if a valid message.
     */
    public void run() {
        String sentence = null;
        byte[] receiveData = null;
        SocketAddress sAddr;

        while (true) {
            receiveData = new byte[1024];
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            try {
                serverSocket.receive(receivePacket);
            } catch (Exception e) {
                e.printStackTrace();
            }

            sentence = new String(receivePacket.getData());

            sentence = sentence.trim();
            
            controller.syncLock.lock();
            try {
                sAddr = receivePacket.getSocketAddress();
            }
            finally { 
                controller.syncLock.unlock();
            }
            int senderPort = receivePacket.getPort();
            if(sentence.startsWith(controller.ASK_STILL_ALIVE))
                askStillAliveResponse(senderPort, sentence, sAddr);
            else if (sentence.equals(controller.STILL_ALIVE))
                stillAliveResponse(senderPort);
        }
    }


    /**
     * Updates predeccessors and sends response message
     * @param senderPort
     * @param sentence The received sentence
     * @param sAddr The socket address of the sender
     */
    private void askStillAliveResponse(int senderPort, String sentence, SocketAddress sAddr) {
        String[] arr = sentence.split(" ");
        if (arr[3].equals("fstSuccessor"))
            controller.setFstPredeccessor(senderPort - 12000);
        else
            controller.setSndPredeccessor(senderPort - 12000);
        controller.pingS(controller.STILL_ALIVE, sAddr);
        System.out.println("Ping request message recieved from Peer " + (senderPort - 12000)); 
    }

    /**
     * Prints out a message claiming ping was received at resets the missed ping counter for the sender.
     * @param senderPort 
     */    
    private void stillAliveResponse(int senderPort) {
        System.out.println("Ping response received from Peer " + (senderPort - 12000));
        if (senderPort - 12000 == controller.getFstSuccessor())
            controller.fstSuccessorMissedPingsReset();
        else
            controller.sndSuccessorMissedPingsReset();
    }
}