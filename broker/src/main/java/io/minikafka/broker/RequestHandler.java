package io.minikafka.broker;

import io.minikafka.protocol.Message;

/**
 * Turns a decoded request into a response. Spec 01 ships only a stub implementation ({@link
 * StubRequestHandler}); real publish/poll/Raft logic replaces it in later specs.
 */
@FunctionalInterface
public interface RequestHandler {

  /** Handles a well-formed request and returns the response to send back. */
  Message handle(Message request);
}
