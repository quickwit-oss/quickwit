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

describe("Home navigation", () => {
  it("Should display sidebar links", () => {
    cy.visit("http://127.0.0.1:7280/ui");
    cy.get("a")
      .should("be.visible")
      .should("contain.text", "Query editor")
      .should("contain.text", "Indexes")
      .should("contain.text", "Cluster");
  });
  it("Should navigate to cluster state", () => {
    cy.visit("http://127.0.0.1:7280/ui");
    cy.get("a").contains("Cluster").click();
    cy.get("p").should("contain.text", "Cluster");
    cy.get("span").should("contain.text", "cluster_id");
  });
  it("Should display otel logs index page", () => {
    cy.visit("http://127.0.0.1:7280/ui/indexes/otel-logs-v0_7");
    cy.get("a").should("be.visible").should("contain.text", "Indexes");
  });
});
