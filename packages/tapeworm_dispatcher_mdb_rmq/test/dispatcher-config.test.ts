import { expect, test } from "vitest";
import { Dispatcher } from "../src/dispatcher";
import { MemoryStore } from "./fixtures";

test.each(["quarantine", "other", Symbol("private-option")])("unsupported own option %s rejects before accessing dependencies", (key) => {
  const config = { get mongodb(): never { throw new Error("Mongo must not be touched"); } };
  Object.defineProperty(config, key, { value: undefined });
  expect(() => { Reflect.construct(Dispatcher, [config]); }).toThrow("Unknown dispatcher option:");
});

test.each([null, false, "handler", {}, { handle: true }, () => undefined])("invalid untyped failureHandler %j rejects before setup", (failureHandler) => {
  const config = { failureHandler, get mongodb(): never { throw new Error("Mongo must not be touched"); } };
  expect(() => { Reflect.construct(Dispatcher, [config]); }).toThrow("failureHandler must be an object with a handle function");
});

test("unsupported option diagnostic does not stringify secret values", () => {
  expect(() => { Reflect.construct(Dispatcher, [{ secret: { toString: () => { throw new Error("secret exposed"); } },
    resumeTokenStore: new MemoryStore() }]); }).toThrow("Unknown dispatcher option: secret;");
});
