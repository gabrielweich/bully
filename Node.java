import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

class NodeProperties {
    int id;
    SocketAddress address;
    boolean connected;

    public NodeProperties(int id, SocketAddress address) {
        this.id = id;
        this.address = address;
        this.connected = false;
    }

    @Override
    public String toString() {
        return Integer.toString(this.id);
    }
}

class MessageProcessor extends Thread {
    DatagramSocket socket;
    Map<Integer, NodeProperties> nodes;
    NodeProperties currentNode;
    NodeProperties coordinator;
    long end = Long.MAX_VALUE;
    long lastCoordinatorCheck;

    public MessageProcessor(DatagramSocket socket, Map<Integer, NodeProperties> nodes, NodeProperties currentNode) {
        this.socket = socket;
        this.nodes = nodes;
        this.currentNode = currentNode;
    }

    public MessageProcessor(DatagramSocket socket, Map<Integer, NodeProperties> nodes, NodeProperties currentNode,
            NodeProperties coordinator) {
        this.socket = socket;
        this.nodes = nodes;
        this.currentNode = currentNode;
        this.coordinator = coordinator;
    }

    public void run() {
        if (this.isCurrentCoordinator())
            end = System.currentTimeMillis() + 10000;

        while (!Thread.interrupted() && System.currentTimeMillis() < end) {
            try {
                if (this.isExternalCoordinator())
                    this.checkCoordinator();
                DatagramPacket packet = Messenger.receive(socket, 500);
                String message = Messenger.extractMessage(packet);
                System.out.println("message > " + message);
                if (message.startsWith("alive"))
                    this.processAlive(packet, message);
                else if (message.startsWith("election"))
                    this.processElection(packet, message);
                else if (message.startsWith("coordinator"))
                    this.processCoordinator(packet, message);
            } catch (IOException e) {
            }
        }
    }

    private boolean isExternalCoordinator(){
        return this.coordinator != null && this.coordinator.id != this.currentNode.id;
    }

    private boolean isCurrentCoordinator() {
        return this.coordinator != null && this.coordinator.id == this.currentNode.id;
    }

    private void checkCoordinator() {
        if (System.currentTimeMillis() - lastCoordinatorCheck > 3000) {
            if (Messenger.isAlive(this.coordinator.address))
                lastCoordinatorCheck = System.currentTimeMillis();
            else {
                try {
                    this.callElection();
                } catch (SocketException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void processElection(DatagramPacket packet, String message) {
        Messenger.sendMessage(socket, packet.getSocketAddress(), "confirm");
        try {
            this.callElection();
        } catch (SocketException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void processAlive(DatagramPacket packet, String message) {
        Messenger.sendMessage(socket, packet.getSocketAddress(), "confirm");
    }

    private void processCoordinator(DatagramPacket packet, String message) {
        int coordinatorId = Integer.parseInt(message.split(";")[1]);
        this.coordinator = this.nodes.get(coordinatorId);
        System.out.println("c " + coordinatorId);
    }

    private void coordinate() throws InterruptedException {
        System.out.println("c " + this.currentNode.id);
        for (NodeProperties node : this.nodes.values()) {
            Messenger.sendMessage(socket, node.address, "coordinator;" + this.currentNode.id);
        }
        this.coordinator = this.currentNode;
        this.end = System.currentTimeMillis() + 10000;
    }

    public void monitor() throws SocketException, InterruptedException {
        System.out.println("will monitor coordinator " + this.coordinator.id);
        while (Messenger.isAlive(this.coordinator.address)) {
            System.out.println("monitoring coordinator");
            Thread.sleep(3000);
        }
        System.out.println("t " + this.coordinator.id);
        this.callElection();
    }

    private void callElection() throws SocketException, InterruptedException {
        System.out.println("e " + Arrays.toString(this.nodes.values().toArray()));
        List<NodeProperties> greaterIdNodes = this.nodes.values().stream().filter(n -> n.id > this.currentNode.id)
                .collect(Collectors.toList());

        for (NodeProperties node : greaterIdNodes) {
            Messenger.sendMessage(socket, node.address, "election;" + this.currentNode.id);
        }

        try {
            Messenger.receive(socket, 500);
            this.monitor();
        } catch (IOException e) {
            this.coordinate();
        }
    }
}

class NodeConnectRunnable implements Runnable {
    SocketAddress targetAddress;

    public NodeConnectRunnable(SocketAddress targetAddress) {
        this.targetAddress = targetAddress;
    }

    @Override
    public void run() {
        while (!Messenger.isAlive(targetAddress)) {
            System.out.println(this.targetAddress + " not responding");
        }
        System.out.println(this.targetAddress.toString() + " connected");
    }

}

class Node {
    DatagramSocket socket;
    Map<Integer, NodeProperties> nodes;
    NodeProperties currentNode;

    public Node() throws IOException {
        this.nodes = new ConcurrentHashMap<>();
    }

    private void readConfig(String filename, int lineNumber) throws FileNotFoundException {
        File myObj = new File(filename);
        Scanner myReader = new Scanner(myObj);
        int currentLine = 1;
        while (myReader.hasNextLine()) {
            String[] tokens = myReader.nextLine().split(" ");
            int nodeId = Integer.parseInt(tokens[0]);
            int nodePort = Integer.parseInt(tokens[2]);
            SocketAddress nodeAddr = new InetSocketAddress(tokens[1], nodePort);
            NodeProperties nodeProperties = new NodeProperties(nodeId, nodeAddr);
            if (currentLine == lineNumber)
                this.currentNode = nodeProperties;
            else
                nodes.put(nodeId, nodeProperties);
            currentLine++;
        }
        myReader.close();
    }

    private NodeProperties getFirstCoordinator() {
        NodeProperties first = this.currentNode;
        for (NodeProperties node : this.nodes.values()) {
            if (node.id > first.id)
                first = node;
        }
        return first;
    }

    private void connect() throws SocketException {
        SocketAddress address = this.currentNode.address;
        this.socket = new DatagramSocket(address);
        System.out.println("Connected at " + address);
    }

    private void waitAllNodesConnect() throws InterruptedException {
        ExecutorService es = Executors.newCachedThreadPool();
        for (NodeProperties target : this.nodes.values()) {
            es.execute(new NodeConnectRunnable(target.address));
        }
        es.shutdown();
        es.awaitTermination(2, TimeUnit.MINUTES);
    }

    public void start(String configFile, int lineNumber) throws Exception {
        this.readConfig(configFile, lineNumber);
        this.connect();
        Thread messageProcessor = new MessageProcessor(socket, nodes, currentNode);
        messageProcessor.start();
        this.waitAllNodesConnect();
        messageProcessor.interrupt();
        NodeProperties firstCoordinator = this.getFirstCoordinator();
        System.out.println("c " + firstCoordinator.id);
        messageProcessor = new MessageProcessor(socket, nodes, currentNode, firstCoordinator);
        messageProcessor.start();
        messageProcessor.join();
        this.socket.close();
    }
}