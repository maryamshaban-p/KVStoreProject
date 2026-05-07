import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import java.util.Scanner;

public class Client {

    public static void main(String[] args) {

        int[] ports = {5001, 5002, 5003};

        Scanner sc = new Scanner(System.in);

        while (true) {

            try {

                RaftNodeInterface leader = null;

                // find leader
                for (int i = 0; i < ports.length; i++) {

                    try {

                        Registry registry =
                                LocateRegistry.getRegistry(
                                        "localhost",
                                        ports[i]);

                        RaftNodeInterface node =
                                (RaftNodeInterface)
                                        registry.lookup(
                                                "Node" + (i + 1));

                        if (node.isLeader()) {

                            leader = node;

                            System.out.println(
                                    "Connected to Leader Node "
                                            + (i + 1));

                            break;
                        }

                    } catch (Exception e) {
                    }
                }

                if (leader == null) {

                    System.out.println("No leader found");
                    continue;
                }

                // user input
                System.out.print("Enter Command: ");

                String line = sc.nextLine();

                String[] parts = line.split(" ");

                // =========================
                // PUT
                // =========================

                if (parts[0].equalsIgnoreCase("PUT")) {

                    if (parts.length < 3) {

                        System.out.println(
                                "Usage: PUT key value");

                        continue;
                    }

                    String key = parts[1];
                    String value = parts[2];

                    String result =
                            leader.put(key, value);

                    System.out.println(result);
                }

                // =========================
                // GET
                // =========================

                else if (parts[0].equalsIgnoreCase("GET")) {

                    if (parts.length < 2) {

                        System.out.println(
                                "Usage: GET key");

                        continue;
                    }

                    String key = parts[1];

                    String result =
                            leader.get(key);

                    System.out.println(
                            "Value = " + result);
                }

                // =========================
                // EXIT
                // =========================

                else if (parts[0].equalsIgnoreCase("EXIT")) {

                    System.out.println("Client Closed");

                    break;
                }

                else {

                    System.out.println(
                            "Unknown Command");
                }

            } catch (Exception e) {

                e.printStackTrace();
            }
        }

        sc.close();
    }
}