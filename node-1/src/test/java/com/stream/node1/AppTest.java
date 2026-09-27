package com.stream.node1;

import com.stream.base.BaseNode;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class AppTest {

    @Test
    public void testAppExtendsBaseNode() {
        App app = new App();
        
        // BaseNode sınıfını extend ettiğini doğrula
        assertTrue(app instanceof BaseNode, "App sınıfı BaseNode sınıfını extend etmelidir.");
        assertEquals("node-1", app.getNodeId(), "Node ID node-1 olmalıdır.");
        
        // Yaşam döngüsünü test et
        app.start();
        assertTrue(app.isRunning());
        
        app.stop();
        assertFalse(app.isRunning());
    }
}
