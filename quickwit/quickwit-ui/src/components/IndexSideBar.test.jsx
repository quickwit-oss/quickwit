import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Client } from "../services/client";
import { IndexSideBar } from "./IndexSideBar";

jest.mock("../services/client");

beforeEach(() => {
  jest.mocked(Client.prototype.listIndexes).mockResolvedValue([]);
});

test("collapses and expands the index explorer", async () => {
  const user = userEvent.setup();
  const onIndexMetadataUpdate = jest.fn();

  const { container } = render(
    <IndexSideBar
      indexMetadata={null}
      onIndexMetadataUpdate={onIndexMetadataUpdate}
    />,
  );

  expect(screen.getByText("Index ID")).toBeInTheDocument();

  const collapseButton = screen.getByRole("button", {
    name: "Collapse index explorer",
  });

  await user.click(collapseButton);

  expect(
    container.querySelector('[data-collapsed="true"]'),
  ).toBeInTheDocument();

  expect(screen.queryByText("Index ID")).not.toBeInTheDocument();

  const expandButton = screen.getByRole("button", {
    name: "Expand index explorer",
  });

  await user.click(expandButton);

  expect(screen.getByText("Index ID")).toBeInTheDocument();

  expect(
    container.querySelector('[data-collapsed="false"]'),
  ).toBeInTheDocument();

  expect(onIndexMetadataUpdate).not.toHaveBeenCalled();
});

test("preserves the selected index after collapsing", async () => {
  const user = userEvent.setup();
  const onIndexMetadataUpdate = jest.fn();

  const indexMetadata = {
    index_config: {
      index_id: "test-index",
      doc_mapping: {
        field_mappings: [],
      },
    },
  };

  render(
    <IndexSideBar
      indexMetadata={indexMetadata}
      onIndexMetadataUpdate={onIndexMetadataUpdate}
    />,
  );

  await waitFor(() => {
    expect(screen.getByRole("combobox")).toHaveValue("test-index");
  });

  await user.click(
    screen.getByRole("button", {
      name: "Collapse index explorer",
    }),
  );

  await user.click(
    screen.getByRole("button", {
      name: "Expand index explorer",
    }),
  );

  await waitFor(() => {
    expect(screen.getByRole("combobox")).toHaveValue("test-index");
  });

  expect(onIndexMetadataUpdate).not.toHaveBeenCalled();
});
