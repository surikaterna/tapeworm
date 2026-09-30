import { expect, test } from "vitest";
import { aggregateTypeHeader } from "../../src/rabbitmq/publisher";
import { commit } from "../support/fixtures";

test.each([
  ["order.created", "order"],
  ["order.created.v2", "order"],
  ["order", "order"],
  ["", undefined],
  [".created", undefined],
  [undefined, undefined],
  [42, undefined],
])("first event type %s yields aggregateType %s", (type, expected) => {
  const value = commit(1);
  value.events = [{ id: "first", type: type as string, payload: {} }];
  expect(aggregateTypeHeader(value)).toBe(expected);
});

test("empty events omit the header; later events never override the first", () => {
  const value = commit(1);
  value.events = [];
  expect(aggregateTypeHeader(value)).toBeUndefined();
  value.events = [{ id: "first", type: "billing.paid", payload: {} },
    { id: "second", type: "shipping.sent", payload: {} }];
  expect(aggregateTypeHeader(value)).toBe("billing");
  value.events = [{ id: "first", type: "", payload: {} },
    { id: "second", type: "shipping.sent", payload: {} }];
  expect(aggregateTypeHeader(value)).toBeUndefined();
});
