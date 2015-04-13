import mousio.etcd4j.EtcdClient;

import java.net.URI;

/**
 * Created by tl on 4/13/15.
 */
public class test_etcd {

    public static void main(String[] args) throws Exception{
        EtcdClient etcd = new EtcdClient(
                URI.create("10.1.14.2:9092")
        );
        

    }
}
