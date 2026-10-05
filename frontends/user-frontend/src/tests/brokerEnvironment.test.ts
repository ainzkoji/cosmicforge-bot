import test from "node:test";
import assert from "node:assert/strict";
import { brokerEnvironment } from "../lib/brokerEnvironment.ts";

test("reconnecting a virtual account preserves all persisted DEMO aliases", () => {
    for (const value of ["demo", "testnet", "sandbox", "paper", "test", "practice", " DEMO "]) {
        assert.equal(brokerEnvironment(value), "demo");
    }
});

test("LIVE aliases remain real-money accounts", () => {
    for (const value of ["live", "mainnet", "production", " LIVE "]) assert.equal(brokerEnvironment(value), "live");
});

test("an unrecognized environment cannot default to real-money", () => {
    for (const value of ["", "unknown", "false"]) assert.throws(() => brokerEnvironment(value), /BROKER_ENVIRONMENT_MISMATCH/);
});
