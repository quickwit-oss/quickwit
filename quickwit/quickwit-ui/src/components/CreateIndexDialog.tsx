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

import {
  Alert,
  Box,
  Button,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
} from "@mui/material";
import { useMemo, useState } from "react";
import { Client } from "../services/client";
import { ResponseError } from "../utils/models";
import { YamlEditor } from "./YamlEditor";

// Starter config offered when the dialog opens. It mirrors the index config
// files accepted by `quickwit index create --index-config`.
export const DEFAULT_INDEX_CONFIG_YAML = `version: 0.9

index_id: my-index

doc_mapping:
  field_mappings:
    - name: timestamp
      type: datetime
      fast: true
      input_formats:
        - rfc3339
      fast_precision: seconds
    - name: body
      type: text
      tokenizer: default
      record: position
      stored: true
  timestamp_field: timestamp

search_settings:
  default_search_fields:
    - body

indexing_settings:
  commit_timeout_secs: 30
`;

const EDITOR_HEIGHT_PX = 420;

export default function CreateIndexDialog({
  open,
  onClose,
  onIndexCreated,
}: Readonly<{
  open: boolean;
  onClose: () => void;
  onIndexCreated: () => void;
}>) {
  const [indexConfig, setIndexConfig] = useState(DEFAULT_INDEX_CONFIG_YAML);
  const [submitting, setSubmitting] = useState(false);
  const [responseError, setResponseError] = useState<ResponseError | null>(
    null,
  );
  const quickwitClient = useMemo(() => new Client(), []);

  const handleClose = () => {
    // Closing mid-flight would leave the dialog unable to report the outcome.
    if (submitting) {
      return;
    }
    setResponseError(null);
    onClose();
  };

  const handleCreate = () => {
    setSubmitting(true);
    setResponseError(null);
    quickwitClient.createIndex(indexConfig).then(
      () => {
        setSubmitting(false);
        setResponseError(null);
        setIndexConfig(DEFAULT_INDEX_CONFIG_YAML);
        onIndexCreated();
      },
      (error) => {
        // Keep the dialog open so the config can be fixed and resubmitted.
        setSubmitting(false);
        setResponseError(error);
      },
    );
  };

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="md" fullWidth>
      <DialogTitle>Create index</DialogTitle>
      <DialogContent>
        <DialogContentText sx={{ fontSize: 14, pb: 1 }}>
          Paste an index config in YAML.
        </DialogContentText>
        <Box
          sx={{
            height: `${EDITOR_HEIGHT_PX}px`,
            border: "1px solid rgba(0, 0, 0, 0.12)",
            borderRadius: 1,
            overflow: "hidden",
          }}
        >
          <YamlEditor value={indexConfig} onChange={setIndexConfig} />
        </Box>
        {responseError !== null && (
          <Alert severity="error" sx={{ mt: 2 }}>
            {responseError.message}
          </Alert>
        )}
      </DialogContent>
      <DialogActions>
        <Button onClick={handleClose} disabled={submitting}>
          Cancel
        </Button>
        <Button
          variant="contained"
          disableElevation
          onClick={handleCreate}
          disabled={submitting || indexConfig.trim().length === 0}
          startIcon={
            submitting ? <CircularProgress size={16} color="inherit" /> : null
          }
        >
          Create
        </Button>
      </DialogActions>
    </Dialog>
  );
}
