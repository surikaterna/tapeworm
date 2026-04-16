import { describe, it, expect } from "vitest";
import { EventStreamSubscriber } from "../src/client";

function getSubscriber() {
  var client = {
    subscribe: function () {
      return { stop: function () {} };
    },
    request: function (
      _opts: unknown,
      cb: (err: Error | null, res: unknown) => void,
    ) {
      cb(null, { commits: [] });
    },
  };
  var versionProvider = function () {
    return Promise.resolve(0);
  };
  return new EventStreamSubscriber(
    client as ConstructorParameters<typeof EventStreamSubscriber>[0],
    versionProvider,
    function () {},
  );
}

describe("EventStreamsubhronizer", function () {
  it("#subscribe should return a handle", function () {
    var sub = getSubscriber();
    var handle = sub.subscribe("stream-1");
    expect(handle).toBeTruthy();
  });
  it("#subscribe should return a handle which you can use to stop subscription", function () {
    var sub = getSubscriber();
    var handle = sub.subscribe("stream-1");
    expect(handle.stop).toBeTruthy();
  });
  it("#subscribe stop should remove subscriptions", function () {
    var sub = getSubscriber();
    var handle = sub.subscribe("stream-1");
    expect(sub.activeStreams().length).toBe(1);
    handle.stop();
    expect(sub.activeStreams().length).toBe(0);
  });
  it("#subscribe stop multiple times should throw", function () {
    var sub = getSubscriber();
    var handle = sub.subscribe("stream-1");
    handle.stop();
    expect(function () {
      handle.stop();
    }).toThrow();
  });
  it("#subscribe multiple times should not increase activeStreams", function () {
    var sub = getSubscriber();
    sub.subscribe("stream-1");
    sub.subscribe("stream-1");
    expect(sub.activeStreams().length).toBe(1);
  });
  it("#stop with multiple subscriptions should not decrease activeStreams", function () {
    var sub = getSubscriber();
    var handle = sub.subscribe("stream-1");
    sub.subscribe("stream-1");
    handle.stop();
    expect(sub.activeStreams().length).toBe(1);
  });
  it("#subscribe on multiple streams increases activeStreams", function () {
    var sub = getSubscriber();
    sub.subscribe("stream-1");
    sub.subscribe("stream-2");
    expect(sub.activeStreams().length).toBe(2);
  });
});
