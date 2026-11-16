// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { expect, test } from "@playwright/test";

let errors: string[];

test.beforeEach(({ page }) => {
  errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("console", (message) => {
    if (message.type() === "error") {
      errors.push(message.text());
    }
  });
});

test.afterEach(() => {
  expect(errors).toEqual([]);
});

test.describe("Views", () => {
  test("Should search, aggregate and open the custom dates panel", async ({
    page,
    request,
  }) => {
    const now = Date.now();
    const ingestResponse = await request.post(
      "/api/v1/otel-logs-v0_9/ingest?commit=force",
      {
        data: [now, now - 2 * 3_600_000]
          .map((millis) =>
            JSON.stringify({
              timestamp_nanos: millis * 1_000_000,
              service_name: "ui-e2e",
              body: { message: "ui e2e log line" },
            }),
          )
          .join("\n"),
      },
    );
    expect(ingestResponse.ok()).toBe(true);
    await page.goto("/ui/search?index_id=otel-logs-v0_9");
    await expect(page.locator(".monaco-editor")).toBeVisible();
    await page.getByRole("button", { name: "Run" }).click();
    await expect(page.getByText("ui e2e log line").first()).toBeVisible();
    await page.getByRole("tab", { name: "Aggregation" }).click();
    await page.getByRole("combobox").filter({ hasText: "1 day" }).click();
    await page.getByRole("option", { name: "1 hour" }).click();
    await page.getByRole("button", { name: "Run" }).click();
    await expect(page.locator(".MuiLineElement-root").first()).toBeVisible();
    await page.getByRole("button", { name: "No date range" }).click();
    await page.getByRole("button", { name: "Custom dates" }).click();
    await expect(page.getByRole("group", { name: "Start Date" })).toBeVisible();
  });

  test("Should list indexes", async ({ page }) => {
    await page.goto("/ui/indexes");
    await expect(
      page.getByText("otel-logs-v0_9", { exact: true }),
    ).toBeVisible();
  });

  test("Should display every index tab", async ({ page }) => {
    await page.goto("/ui/indexes/otel-logs-v0_9");
    for (const name of [
      "Summary",
      "Sources",
      "Doc Mapping",
      "Indexing settings",
      "Search settings",
      "Retention settings",
      "Splits",
    ]) {
      await page.getByRole("tab", { name }).click();
      await expect(page.getByRole("tabpanel")).toBeVisible();
    }
  });

  test("Should display node info", async ({ page }) => {
    await page.goto("/ui/node-info");
    await expect(page.locator(".monaco-editor")).toBeVisible();
    await page.getByRole("tab", { name: "Build info" }).click();
    await expect(page.locator(".monaco-editor")).toBeVisible();
  });

  test("Should render the API playground", async ({ page }) => {
    await page.goto("/ui/api-playground");
    await expect(page.locator(".swagger-ui .opblock").first()).toBeVisible();
  });
});
