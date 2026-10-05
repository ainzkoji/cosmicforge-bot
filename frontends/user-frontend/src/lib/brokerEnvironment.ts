export function brokerEnvironment(value: string): "demo" | "live" {
    const environment = value.trim().toLowerCase();
    if (["demo", "testnet", "sandbox", "paper", "test", "practice"].includes(environment)) return "demo";
    if (["live", "mainnet", "production"].includes(environment)) return "live";
    throw new Error("BROKER_ENVIRONMENT_MISMATCH");
}
