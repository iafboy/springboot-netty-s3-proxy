package com.example;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.io.FileInputStream;
import java.security.KeyStore;

public class SSLFactory {
    public static SSLContext getSslContext(char[] passArray,String jskFilePath) throws Exception{
        SSLContext sslContext=SSLContext.getInstance("TLSv1");
        KeyStore ks=KeyStore.getInstance("JKS");
        FileInputStream inputStream= new FileInputStream(jskFilePath);
        ks.load(inputStream,passArray);
        KeyManagerFactory kmf=KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks,passArray);
        sslContext.init(kmf.getKeyManagers(),null,null);
        inputStream.close();
        return sslContext;
    }
}
