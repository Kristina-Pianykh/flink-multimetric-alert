import java.io.*;
import java.net.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class SocketSink<Event> extends RichSinkFunction<Event> {
  private transient Socket socket;
  private transient ObjectOutputStream objectOutputStream;
  int port = 6667;
  String hostname = "localhost";

  @Override
  public void open(Configuration parameters) throws Exception {
    this.socket = openSocket(hostname, port);
    this.objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
  }

  public static Socket openSocket(String hostname, int port) {
    Socket socket = null;
    while (socket == null) {
      try {
        socket = new Socket(hostname, port);
        System.out.println("Connected to " + hostname + " on " + socket.getInetAddress());
        break;
      } catch (UnknownHostException e) {
        e.printStackTrace();
        System.exit(-1);
      } catch (IOException e) {
        System.err.println("Failed to connect to " + hostname + " on port " + port);
      }
    }
    return socket;
  }

  @Override
  public void close() throws Exception {
    if (objectOutputStream != null) {
      objectOutputStream.close();
    }
    if (socket != null) {
      socket.close();
    }
  }

  @Override
  public void invoke(Event value, Context ctx) throws Exception {
    try {
      objectOutputStream.writeObject(value);
      objectOutputStream.flush();
      // OutputStream outputStream = socket.getOutputStream();
      // ObjectOutputStream socketOutputStream = new ObjectOutputStream(outputStream);
      // socketOutputStream.writeObject(value);
      // socketOutputStream.flush(); // ??
      System.out.printf("Sent %s to the socket \n", value);
    } catch (SocketException e) {
      System.err.println("Failed to send " + value.toString());
      System.err.println("Failed to connect to socket cause connection was closed by the server");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
