import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface RaftNodeInterface extends Remote {

    boolean requestVote(int term, int candidateId)
            throws RemoteException;

    boolean appendEntries(int term,
                          int leaderId,
                          List<String> entries)
            throws RemoteException;

    String put(String key, String value)
            throws RemoteException;

    String get(String key)
            throws RemoteException;

    boolean isLeader()
            throws RemoteException;
}