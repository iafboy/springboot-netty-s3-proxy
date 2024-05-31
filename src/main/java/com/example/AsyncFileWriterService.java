package com.example;

import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Service
public class AsyncFileWriterService {

    public CompletableFuture<Void> writeFile(String filePath, byte[] content) {
        return CompletableFuture.runAsync(() -> {
            Path path = Path.of(filePath);
            ByteBuffer buffer = ByteBuffer.wrap(content);
            try (AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(
                    path, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
                fileChannel.write(buffer, 0).get();
            } catch (IOException | InterruptedException | ExecutionException e) {
                throw new RuntimeException("Failed to write file", e);
            }
        });
    }
}

