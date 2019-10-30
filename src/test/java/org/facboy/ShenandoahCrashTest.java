package org.facboy;

import com.google.common.io.Files;
import org.junit.jupiter.api.Test;

class ShenandoahCrashTest {

    @Test
    @SuppressWarnings("UnstableApiUsage")
    void testServer() throws Exception {
        KafkaTestServer server = new KafkaTestServer(Files.createTempDir());
        server.start();

        Thread.sleep(5000);

        server.stop();
    }
}
