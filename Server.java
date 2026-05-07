import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import java.util.HashMap;
import java.util.Map;

public class Server {

    public static void main(String[] args) {

        try {

            int nodeId =
                    Integer.parseInt(args[0]);

            int port =
                    Integer.parseInt(args[1]);

            Registry registry =
                    LocateRegistry.createRegistry(port);

            Map<Integer, Integer> peers =
                    new HashMap<>();

            peers.put(1, 5001);
            peers.put(2, 5002);
            peers.put(3, 5003);

            peers.remove(nodeId);

            RaftNode node =
                    new RaftNode(nodeId, peers);

            registry.rebind(
                    "Node" + nodeId,
                    node);

            System.out.println(
                    "Node " + nodeId +
                            " running on port "
                            + port);

        } catch (Exception e) {

            e.printStackTrace();
        }
    }
}