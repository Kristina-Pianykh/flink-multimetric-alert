import java.io.*;
import java.net.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class SocketSource extends RichSourceFunction<Tuple2<String, Integer>> {

  private volatile boolean isRunning = true;
  private int port = 6666;
  public static boolean pattern2Enabled = true;

  public SocketSource(int port) {
    this.port = port;
  }

  @Override
  public void run(SourceContext<Tuple2<String, Integer>> sourceContext) throws Exception {
    try (ServerSocket serverSocket = new ServerSocket(port)) {
      System.out.println(
          String.format("Server started. Listening for connections on port %d...", port));

      while (this.isRunning) {
        Socket socket = serverSocket.accept();
        new ClientHandler(socket, sourceContext).start(); // Hand off to a new thread
      }
    } catch (IOException e) {
      e.printStackTrace(); // TODO: handle exception
      System.exit(1);
    }
  }

  @Override
  public void cancel() {
    this.isRunning = false; // any sockets/readers to close?
  }

  private static class ClientHandler extends Thread {
    private SourceContext<Tuple2<String, Integer>> sourceContext;
    private Socket socket;

    public ClientHandler(Socket socket, SourceContext<Tuple2<String, Integer>> sourceContext) {
      this.sourceContext = sourceContext;
      this.socket = socket;
    }

    @Override
    public void run() {
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
        System.out.println("Socket for the connection: " + socket.getInetAddress() + " is open.");

        String line;
        while ((line = reader.readLine()) != null) {
          System.out.println("Received line: " + line);
          // Expecting input in the format "name"
          // if (line.equals("control")) {
          //   pattern2Enabled = !pattern2Enabled;
          // }
          String[] parts = line.split(",");
          Tuple2<String, Integer> event = new Tuple2<>(parts[0], Integer.parseInt(parts[1]));
          System.out.println(event);
          sourceContext.collect(event);
        }
      } catch (IOException ex) {
        ex.printStackTrace();
      } finally {
        try {
          socket.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
