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

test.describe("Home navigation", () => {
  test("Should display sidebar links", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator("a")).toContainText([
      "Query editor",
      "Indexes",
      "Cluster",
    ]);
  });

  test("Should navigate to cluster state", async ({ page }) => {
    await page.goto("/");
    await page.getByRole("link", { name: "Cluster" }).click();
    await expect(page.getByLabel("breadcrumb")).toContainText("Cluster");
    await expect(page.getByText("cluster_id")).toBeVisible();
  });

  test("Should display otel logs index page", async ({ page }) => {
    await page.goto("/ui/indexes/otel-logs-v0_7");
    await expect(
      page.getByLabel("breadcrumb").getByRole("link", { name: "Indexes" }),
    ).toBeVisible();
  });

});
