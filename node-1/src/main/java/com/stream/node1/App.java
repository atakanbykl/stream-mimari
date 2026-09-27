package com.stream.node1;

import com.stream.base.BaseNode;

/**
 * node-1 servisi ana sınıfı. BaseNode sınıfını extend eder.
 */
public class App extends BaseNode {

    public App() {
        super("node-1");
    }

    public App(String natsUrl) {
        super("node-1", natsUrl);
    }

    @Override
    protected void onStart() throws Exception {
        log("Servis dinlemeye ve veri işlemeye başladı.");
        // İlgili topic'lere abonelik ve stream işlemleri burada başlatılabilir
    }

    @Override
    protected void onStop() throws Exception {
        log("Servis bağlantıları ve kaynakları kapatıldı.");
    }

    public static void main(String[] args) {
        App node = new App();
        
        // Kapatma sinyalinde (SIGINT / Ctrl+C) stop() metodunun tetiklenmesi
        Runtime.getRuntime().addShutdownHook(new Thread(node::stop));

        node.start();
    }
}
