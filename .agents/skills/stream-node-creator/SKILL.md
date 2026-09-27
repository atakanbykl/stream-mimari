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
```java
package com.stream.nodeX;

import com.stream.base.BaseNode;
import io.nats.client.Message;

public class App extends BaseNode {

    public App() {
        super("node-X");
    }

    @Override
    protected void onStart() throws Exception {
        log("Node-X başlatıldı. Abonelikler ve akışlar hazırlanıyor...");
        
        // Örnek: JetStream stream'inin varlığını garantiye al
        ensureStream("STREAM_NODES", "stream.orders.*", "stream.alerts");

        // Örnek: Core NATS aboneliği
        subscribeCore("core.nodeX.input", msg -> {
            log("Core mesajı alındı: " + new String(msg.getData()));
        });

        // Örnek: JetStream aboneliği
        subscribeStream("stream.orders.new", msg -> {
            log("Sipariş işlendi.");
            msg.ack();
        }, false);
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
