import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketAddress;

public class Messenger {
    public static boolean sendMessage(DatagramSocket socket, SocketAddress address, String message) {
        byte[] command = new byte[1024];
        command = message.getBytes();
        try {
            socket.send(new DatagramPacket(command, command.length, address));
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean isAlive(SocketAddress address) {
        try {
            DatagramSocket socket = new DatagramSocket();
            Messenger.sendMessage(socket, address, "alive");
            socket.setSoTimeout(1000);
            byte[] recBuff = new byte[1024];
            DatagramPacket recPacket = new DatagramPacket(recBuff, recBuff.length);
            socket.receive(recPacket);
            socket.close();
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}