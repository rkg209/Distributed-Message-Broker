package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.ErrorResp;
import io.minikafka.protocol.Frame;
import io.minikafka.protocol.FrameDecoder;
import io.minikafka.protocol.FrameEncoder;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.MessageCodec;
import io.minikafka.protocol.MessageType;
import io.minikafka.protocol.MetadataReq;
import io.minikafka.protocol.MetadataResp;
import io.minikafka.protocol.ProtocolConfig;
import io.minikafka.protocol.PublishReq;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** AC-1, AC-2, AC-3: connect, METADATA round-trip, and malformed-frame resilience. */
class ConnectionAcceptorTest {

  private static final int MAX = ProtocolConfig.DEFAULT_MAX_FRAME_BYTES;
  private final MessageCodec codec = MessageCodec.instance();

  private ConnectionAcceptor acceptor;

  @BeforeEach
  void startServer() throws IOException {
    BrokerInfo self = new BrokerInfo(1, "localhost", 9092);
    acceptor = new ConnectionAcceptor(0, MAX, new StubRequestHandler(self));
    acceptor.start();
  }

  @AfterEach
  void stopServer() {
    acceptor.close();
  }

  private Socket connect() throws IOException {
    Socket socket = new Socket();
    socket.connect(new InetSocketAddress("localhost", acceptor.boundPort()));
    return socket;
  }

  @Test
  @Timeout(10)
  void clientCanConnect() throws IOException {
    try (Socket socket = connect()) {
      assertTrue(socket.isConnected());
    }
  }

  @Test
  @Timeout(10)
  void metadataRequestGetsStubResponse() throws IOException {
    try (Socket socket = connect();
        InputStream in = socket.getInputStream();
        OutputStream out = socket.getOutputStream()) {
      new FrameEncoder(out).write(codec.encode(new MetadataReq(77L)));

      Message response = codec.decode(new FrameDecoder(in, MAX).read());
      MetadataResp meta = assertInstanceOf(MetadataResp.class, response);
      assertEquals(77L, meta.correlationId());
      assertEquals(1, meta.brokers().size());
      assertEquals(9092, meta.brokers().get(0).port());
    }
  }

  @Test
  @Timeout(10)
  void unsupportedRequestGetsErrorResp() throws IOException {
    try (Socket socket = connect();
        InputStream in = socket.getInputStream();
        OutputStream out = socket.getOutputStream()) {
      new FrameEncoder(out).write(codec.encode(new PublishReq(5L, "t", 0, null, new byte[] {1})));

      Message response = codec.decode(new FrameDecoder(in, MAX).read());
      ErrorResp error = assertInstanceOf(ErrorResp.class, response);
      assertEquals(5L, error.correlationId());
      assertEquals(ErrorResp.CODE_UNSUPPORTED, error.errorCode());
    }
  }

  @Test
  @Timeout(10)
  void malformedFrameYieldsErrorRespAndServerSurvives() throws IOException {
    // Send a frame with an unknown type byte; the server should reply ERROR_RESP and not die.
    try (Socket socket = connect();
        InputStream in = socket.getInputStream();
        OutputStream out = socket.getOutputStream()) {
      out.write(new byte[] {0, 0, 0, 1, 0x00}); // length=1, unknown type 0x00
      out.flush();

      Frame frame = new FrameDecoder(in, MAX).read();
      assertEquals(MessageType.ERROR_RESP, frame.type());
      ErrorResp error = (ErrorResp) codec.decode(frame);
      assertEquals(ErrorResp.CODE_PROTOCOL_ERROR, error.errorCode());
    }

    // A brand-new connection must still be served — the acceptor survived the bad frame.
    try (Socket socket = connect();
        InputStream in = socket.getInputStream();
        OutputStream out = socket.getOutputStream()) {
      new FrameEncoder(out).write(codec.encode(new MetadataReq(1L)));
      Message response = codec.decode(new FrameDecoder(in, MAX).read());
      assertInstanceOf(MetadataResp.class, response);
    }
  }

  @Test
  @Timeout(10)
  void oversizedFrameIsRejected() throws IOException {
    // A server with a tiny frame cap must reject an over-long declared length without reading a
    // body.
    ConnectionAcceptor smallAcceptor =
        new ConnectionAcceptor(0, 1024, new StubRequestHandler(new BrokerInfo(2, "h", 1)));
    smallAcceptor.start();
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", smallAcceptor.boundPort()));
      socket.getOutputStream().write(new byte[] {0x00, 0x10, 0x00, 0x00}); // ~1 MB declared
      socket.getOutputStream().flush();

      Frame frame = new FrameDecoder(socket.getInputStream(), MAX).read();
      assertEquals(MessageType.ERROR_RESP, frame.type());
    } finally {
      smallAcceptor.close();
    }
  }
}
