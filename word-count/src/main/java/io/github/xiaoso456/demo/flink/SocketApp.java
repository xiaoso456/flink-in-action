package io.github.xiaoso456.demo.flink;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

public class SocketApp {
    public static void main(String[] args) throws IOException, InterruptedException {

        ServerSocket serverSocket = new ServerSocket(8888);

        Socket socket = serverSocket.accept();
        System.out.println("success create connection from " + socket.getInetAddress() + ":" + socket.getPort());

        OutputStream out = socket.getOutputStream();
        PrintWriter writer = new PrintWriter(out, true);
        Scanner scanner = new Scanner(System.in);
        while (true) {
            writer.println(scanner.nextLine());
        }
    }
}
