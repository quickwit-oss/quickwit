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

import CheckIcon from "@mui/icons-material/Check";
import EditIcon from "@mui/icons-material/Edit";
import { Box, Button, Chip, Stack } from "@mui/material";
import { useEffect, useState } from "react";
import { JsonEditor } from "./JsonEditor";

type JsonEditorEditableProps = {
  saving: boolean;
  pristine: boolean;
  onSave: () => void;
} & React.ComponentProps<typeof JsonEditor>;

/**
 * wrapper around JsonEditor that displays edit actions
 */
export function JsonEditorEditable({
  saving,
  pristine,
  onSave,
  ...jsonEditorProps
}: JsonEditorEditableProps) {
  const wasSaving = useDelayedTruthyValue(saving, 1000);
  const showSuccess = !saving && wasSaving;

  return (
    <Box style={{ height: "100%", position: "relative" }}>
      <JsonEditor readOnly={false} {...jsonEditorProps} />
      <Stack
        direction="row"
        spacing={1}
        style={{ position: "absolute", top: 8, right: 12, zIndex: 1001 }}
      >
        {pristine && !showSuccess && (
          <Chip
            icon={<EditIcon sx={{ fontSize: 16 }} />}
            label="Editable"
            size="small"
            variant="filled"
          />
        )}
        {(!pristine || showSuccess) && (
          <Button
            variant="contained"
            size="small"
            onClick={onSave}
            disabled={saving || showSuccess}
            style={{ width: "100px" }}
          >
            {saving && "Saving..."}
            {showSuccess && (
              <>
                <CheckIcon sx={{ fontSize: 16, mr: 0.5 }} />
                Saved
              </>
            )}
            {!saving && !showSuccess && "Save"}
          </Button>
        )}
      </Stack>
    </Box>
  );
}

/**
 * Returns the value immediately when truthy, but delays returning falsy values by delayMs.
 * Useful for showing success states briefly after an operation completes.
 */
function useDelayedTruthyValue<T>(value: T, delayMs: number): T {
  const [delayedValue, setDelayedValue] = useState<T>(value);

  useEffect(() => {
    if (value) {
      setDelayedValue(value);
    } else {
      const timeout = setTimeout(() => {
        setDelayedValue(value);
      }, delayMs);

      return () => clearTimeout(timeout);
    }
  }, [value, delayMs]);

  return value || delayedValue;
}
