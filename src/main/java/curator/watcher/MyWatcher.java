package curator.watcher;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class MyWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
        System.out.println("触发 watcher，节点路径为：" + event.getPath());
    }
}
