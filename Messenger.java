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
            Messenger.receive(socket, 1000);
            socket.close();
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public static String extractMessage(DatagramPacket packet) {
        return new String(packet.getData(), 0, packet.getLength());
    }

    public static DatagramPacket receive(DatagramSocket socket, int timeout) throws IOException {
        byte[] text = new byte[1024];
        DatagramPacket packet = new DatagramPacket(text, text.length);
        socket.setSoTimeout(timeout);
        socket.receive(packet);
        return packet;
    }
}