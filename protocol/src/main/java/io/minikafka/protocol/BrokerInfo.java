package io.minikafka.protocol;

/** A broker's identity and address, as advertised in a {@link MetadataResp}. */
public record BrokerInfo(int brokerId, String host, int port) {

  public BrokerInfo {
    if (host == null) {
      throw new IllegalArgumentException("host must not be null");
    }
  }
}
