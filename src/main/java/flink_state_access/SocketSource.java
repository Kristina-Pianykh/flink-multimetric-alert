import java.io.*;
import java.net.*;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class SocketSource extends RichSourceFunction<Event> {

  private volatile boolean isRunning = true;
  private String hostname = "localhost";
  private int port = 6666;
  public static boolean pattern2Enabled = true;

  public SocketSource(int port) {
    this.port = port;
  }

  @Override
  public void run(SourceContext<Event> sourceContext) throws Exception {
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
    private SourceContext<Event> sourceContext;
    private Socket socket;

    public ClientHandler(Socket socket, SourceContext<Event> sourceContext) {
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
          // Expecting input in the format "name,timestamp"
          String[] parts = line.split(",");
          if (parts.length == 2) {
            String name = parts[0];
            long timestamp = Long.parseLong(parts[1].trim());
            if (name.equals("control") && timestamp == 1) {
              pattern2Enabled = !pattern2Enabled;
            }
            System.out.println(new Event(name, timestamp, pattern2Enabled));
            sourceContext.collect(new Event(name, timestamp, pattern2Enabled));
          }
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
