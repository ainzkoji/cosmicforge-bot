/** Section L-05 -- placeholder pages say what they are and show no invented content or controls. */
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";

const src = (relative: string) => readFileSync(join(process.cwd(), "src", relative), "utf8");

for (const page of ["pages/Academy.tsx", "pages/DeveloperSettings.tsx"]) {
    test(`${page} is an honest placeholder`, () => {
        const text = src(page);
        assert.match(text, /Not available yet/);
        assert.match(text, /role="status"/);
        // No mock data, fake progress, buttons or fake click handlers.
        for (const forbidden of [/Mock Data/i, /<button/, /<input/, /onClick=/, /alert\(/, /useState/]) {
            assert.doesNotMatch(text, forbidden, `${page} contains ${forbidden}`);
        }
    });
}
