
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

    public void run() {
        String sentence = null;
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
            controller.dbg("received: " + sentence);
            
            // syncLock.lock();
            try {
                sAddr = receivePacket.getSocketAddress();
            }
            finally { 
                // syncLock.unlock();
            }
            String[] arr = sentence.split(" ");
            if(sentence.startsWith(controller.ASK_STILL_ALIVE)) {
                int senderId = receivePacket.getPort();
                if (arr[3].equals("fstSuccessor")) {
                    controller.setFstPredeccessor(senderId - 12000);
                    controller.fstSuccessorMissedPingsReset();
                } else {
                    controller.setSndPredeccessor(senderId - 12000);
                    controller.sndSuccessorMissedPingsReset();
                }
                controller.pingS(controller.STILL_ALIVE, sAddr);
                System.out.println("Ping request message recieved from Peer " + (senderId - 12000));
            }
            else if (sentence.equals(controller.STILL_ALIVE)) {
                int senderId = receivePacket.getPort();
                System.out.println("Ping response received from Peer " + (senderId - 12000)); 
            } 
        }
    }
}