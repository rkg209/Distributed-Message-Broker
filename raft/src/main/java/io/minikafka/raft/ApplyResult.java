package io.minikafka.raft;

/**
 * Outcome of applying one committed entry to the {@link StateMachine}. Exactly one of {@code
 * value}/{@code error} is non-null — an application failure must surface to the caller of {@link
 * RaftNode#propose}, never be swallowed.
 */
public record ApplyResult(byte[] value, String error) {

  public ApplyResult {
    if ((value == null) == (error == null)) {
      throw new IllegalArgumentException("exactly one of value/error must be non-null");
    }
    if (value != null) {
      value = value.clone();
    }
  }

  public static ApplyResult ok(byte[] value) {
    return new ApplyResult(value, null);
  }

  public static ApplyResult error(String message) {
    return new ApplyResult(null, message);
  }

  public boolean isOk() {
    return error == null;
  }

  @Override
  public byte[] value() {
    return value == null ? null : value.clone();
  }
}
