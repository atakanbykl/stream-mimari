---
name: stream-node-creator
description: >-
  Use this skill when the user asks to create, scaffold, or add a new node service (such as node-2, node-3, etc.)
  in the stream-mimari project. Generates the Maven submodule, extends BaseNode, registers the module in the parent
  pom.xml, and configures Core and JetStream pub/sub handlers.
---

# Stream Node Creator Skill

Bu beceri, `stream-mimari` projesinde standartlara uygun yeni bir `node-*` servisini (ör. `node-2`, `node-3`) otomatik olarak oluşturmak için kullanılır.

## İş Akışı Adımları

Yeni bir node oluşturulması istendiğinde şu adımları sırayla takip edin:

### 1. Kök `pom.xml` Dosyasına Modülü Kaydetme
`stream-mimari/pom.xml` dosyasındaki `<modules>` etiketinin içerisine yeni düğümü ekleyin:
```xml
<modules>
    <module>base</module>
    <module>node-1</module>
    <module>node-X</module> <!-- Yeni eklenen -->
</modules>
```

### 2. Dizin Yapısını Oluşturma
Yeni node için standart Maven klasör yapısını kurun:
```text
node-X/
├── pom.xml
├── .gitignore
├── README.md
└── src/
    ├── main/java/com/stream/nodeX/App.java
    └── test/java/com/stream/nodeX/AppTest.java
```

### 3. `node-X/pom.xml` Dosyasını Oluşturma
Parent olarak `stream-mimari` referans verilmeli ve `base` bağımlılığı eklenmelidir:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.stream</groupId>
        <artifactId>stream-mimari</artifactId>
        <version>1.0-SNAPSHOT</version>
        <relativePath>../pom.xml</relativePath>
    </parent>

    <artifactId>node-X</artifactId>
    <packaging>jar</packaging>

    <name>node-X</name>
    <description>Stream mimarisi node-X servisi</description>

    <properties>
        <exec.mainClass>com.stream.nodeX.App</exec.mainClass>
    </properties>

    <dependencies>
        <dependency>
            <groupId>com.stream</groupId>
            <artifactId>base</artifactId>
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.codehaus.mojo</groupId>
                <artifactId>exec-maven-plugin</artifactId>
                <version>3.5.0</version>
                <configuration>
                    <mainClass>${exec.mainClass}</mainClass>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

### 4. `App.java` Sınıfını `BaseNode`'dan Extend Ederek Oluşturma
Yeni düğüm, çift katmanlı mimari standartlarına (NATS Core anlık + JetStream geçmiş hafızası) tam uyumlu olarak üretilmelidir:
```java
package com.stream.nodeX;

import com.stream.base.BaseNode;
import com.stream.base.StreamRecord;
import com.stream.base.model.*;
import io.nats.client.Message;
import java.util.Optional;

public class App extends BaseNode {

    public static class NodeXData {
        public String nodeId;
        public String status;
        public long timestamp;

        public NodeXData() {}
        public NodeXData(String nodeId, String status) {
            this.nodeId = nodeId;
            this.status = status;
            this.timestamp = System.currentTimeMillis();
        }
    }

    public App() {
        super("node-X");
    }

    @Override
    protected void onStart() throws Exception {
        log("Node-X başlatıldı. Çift katmanlı akış hazırlanıyor...");
        
        // 1. JetStream merkezi stream'ini garantiye al (allowDirect=true ile)
        ensureDefaultStream();

        // 2. NATS Core: Minimum gecikmeli anlık komut dinleme
        subscribeCore("core.nodeX.commands", msg -> {
            log("Core komut mesajı alındı: " + new String(msg.getData()));
        });

        // 3. JetStream: Canlı yeni olayları dinleme (DeliverPolicy.New ile eski geçmişi tekrar etmez)
        subscribeStreamLiveJson("stream.nodeX.events", NodeXData.class, (data, msg) -> {
            log("Canlı stream olayı yakalandı: " + data.status);
        }, true);

        // 4. Örnek: Geçmişten son durumu çekme (Direct Get)
        Optional<StreamRecord<NodeXData>> lastState = getLastMessageRecord("stream.nodeX.events", NodeXData.class);
        lastState.ifPresent(rec -> log("Geçmişteki son durum: " + rec.getPayload().status));
    }

    @Override
    protected void onStop() throws Exception {
        log("Node-X kaynakları temizlendi.");
    }

    public static void main(String[] args) {
        App node = new App();
        Runtime.getRuntime().addShutdownHook(new Thread(node::stop));
        node.start();
    }
}
```

### 5. Doğrulama (Verification)
Oluşturulan yeni modülün derlendiğini doğrulamak için terminalde derleme testi yapın:
```powershell
$cp = "c:\Users\ataka\OneDrive\Desktop\CodeFolders\Java\NetBeans\demo-fkb\libs\jnats-2.23.0.jar;c:\Users\ataka\OneDrive\Desktop\CodeFolders\Java\NetBeans\demo-fkb\libs\gson-2.9.1.jar"
javac -cp $cp -d base/target/classes base/src/main/java/com/stream/base/*.java
javac -cp "$cp;base/target/classes" -d node-X/target/classes node-X/src/main/java/com/stream/nodeX/App.java
Remove-Item -Recurse -Force base/target, node-X/target
```
