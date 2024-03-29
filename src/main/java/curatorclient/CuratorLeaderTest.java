package curatorclient;


import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.EnsurePath;

/**
 * Curator framework's leader election test.
 * Output:
 *  LeaderSelector-2 take leadership!
 *  LeaderSelector-2 relinquish leadership!
 *  LeaderSelector-1 take leadership!
 *  LeaderSelector-1 relinquish leadership!
 *  LeaderSelector-0 take leadership!
 *  LeaderSelector-0 relinquish leadership!
 *      ...
 */
public class CuratorLeaderTest {

    /** Zookeeper info */
    private static final String ZK_ADDRESS = "localhost:2181";
    private static final String ZK_PATH = "/zktest";

    public static void main(String[] args) throws InterruptedException {
        LeaderSelectorListener listener = new LeaderSelectorListener() {
            @Override
            public void takeLeadership(CuratorFramework client) throws Exception {
                System.out.println(Thread.currentThread().getName() + " take leadership!");

                // takeLeadership() method should only return when leadership is being relinquished.
                Thread.sleep(500L);

                System.out.println(Thread.currentThread().getName() + " relinquish leadership!");

            }

            @Override
            public void stateChanged(CuratorFramework client, ConnectionState state) {
            }
        };
        RegisterListener r1 = new RegisterListener(listener);
        new Thread(r1).start();
        RegisterListener r2 = new RegisterListener(listener);

        new Thread(r2).start();
        RegisterListener r3 = new RegisterListener(listener);

        new Thread(r3).start();

        Thread.sleep(Integer.MAX_VALUE);
    }

    private static class  RegisterListener implements Runnable  {
        // 1.Connect to zk
        private LeaderSelectorListener listener;
        public RegisterListener(LeaderSelectorListener listener){
            this.listener = listener;
        }
        @Override
        public void run() {

            CuratorFramework client = CuratorFrameworkFactory.newClient(
                    ZK_ADDRESS,
                    new RetryNTimes(10, 500)
            );
            client.start();

            // 2.Ensure path
            try {
                new EnsurePath(ZK_PATH).ensure(client.getZookeeperClient());
            } catch (Exception e) {
                e.printStackTrace();
            }

            // 3.Register listener
            LeaderSelector selector = new LeaderSelector(client, ZK_PATH, listener);
            selector.autoRequeue();
            selector.start();
        }

    }

}
