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

import styled from "@emotion/styled";
import { ChevronRight, KeyboardArrowDown } from "@mui/icons-material";
import {
  Autocomplete,
  Box,
  Chip,
  CircularProgress,
  IconButton,
  List,
  ListItem,
  ListItemText,
  TextField,
  Typography,
} from "@mui/material";
import Tooltip from "@mui/material/Tooltip";
import React, { useEffect, useMemo, useState } from "react";
import { Client } from "../services/client";
import { FieldMapping, getAllFields, IndexMetadata } from "../utils/models";

const IndexBarWrapper = styled("div")({
  display: "flex",
  height: "100%",
  flex: "0 0 260px",
  maxWidth: "260px",
  flexDirection: "column",
  borderRight: "1px solid rgba(0, 0, 0, 0.12)",
  overflow: "auto",
});

function IndexAutocomplete(props: IndexMetadataProps) {
  const [open, setOpen] = React.useState(false);
  const [options, setOptions] = React.useState<readonly IndexMetadata[]>([]);
  const [value, setValue] = React.useState<IndexMetadata | null>(null);
  const [loading, setLoading] = React.useState(false);
  // We want to show the circular progress only if we are loading some results and
  // when there is no option available.
  const showLoading = loading && options.length === 0;
  const quickwitClient = useMemo(() => new Client(), []);

  useEffect(() => {
    if (loading) {
      return;
    }
    setLoading(true);
    quickwitClient.listIndexes().then(
      (indexesMetadata) => {
        setOptions([...indexesMetadata]);
        setLoading(false);
      },
      (error) => {
        console.log("Index autocomplete error", error);
        setLoading(false);
      },
    );
  }, [quickwitClient, open]);

  useEffect(() => {
    if (!open) {
      if (props.indexMetadata !== null && options.length === 0) {
        setOptions([props.indexMetadata]);
      }
    }
  }, [open, props.indexMetadata, options.length]);

  useEffect(() => {
    setValue(props.indexMetadata);
  }, [props.indexMetadata]);

  return (
    <Autocomplete
      size="small"
      sx={{ width: 210 }}
      open={open}
      value={value}
      onChange={(_, updatedValue) => {
        setValue(updatedValue);

        if (
          updatedValue == null ||
          updatedValue.index_config.index_id == null
        ) {
          props.onIndexMetadataUpdate(null);
        } else {
          props.onIndexMetadataUpdate(updatedValue);
        }
      }}
      onOpen={() => {
        setOpen(true);
      }}
      onClose={() => {
        setOpen(false);
        setLoading(false);
      }}
      isOptionEqualToValue={(option, value) =>
        option.index_config.index_id === value.index_config.index_id
      }
      getOptionLabel={(option) => option.index_config.index_id}
      options={options}
      noOptionsText="No indexes."
      loading={loading}
      renderInput={(params) => (
        <TextField
          {...params}
          placeholder="Select an index"
          InputProps={{
            ...params.InputProps,
            endAdornment: (
              <React.Fragment>
                {showLoading ? (
                  <CircularProgress color="inherit" size={20} />
                ) : null}
                {params.InputProps.endAdornment}
              </React.Fragment>
            ),
          }}
        />
      )}
    />
  );
}

export interface IndexMetadataProps {
  indexMetadata: null | IndexMetadata;
  onIndexMetadataUpdate(indexMetadata: IndexMetadata | null): void;
}

function fieldTypeLabel(fieldMapping: FieldMapping): string {
  if (fieldMapping.type[0] !== undefined) {
    return fieldMapping.type[0].toUpperCase();
  } else {
    return "";
  }
}

export function IndexSideBar(props: IndexMetadataProps) {
  const [open, setOpen] = useState(true);
  const fields =
    props.indexMetadata === null
      ? []
      : getAllFields(
          props.indexMetadata.index_config.doc_mapping.field_mappings,
        );
  return (
    <IndexBarWrapper>
      <Box sx={{ px: 3, py: 2 }}>
        <Typography variant="body1" mb={1}>
          Index ID
        </Typography>
        <IndexAutocomplete {...props} />
      </Box>
      <Box sx={{ paddingLeft: "10px", height: "100%" }}>
        <IconButton
          aria-label="expand row"
          size="small"
          onClick={() => setOpen(!open)}
        >
          {open ? <KeyboardArrowDown /> : <ChevronRight />}
        </IconButton>
        Fields
        {open && (
          <List
            dense={true}
            sx={{ paddingTop: "0", overflowWrap: "break-word" }}
          >
            {fields.map(function (field) {
              return (
                <ListItem
                  key={field.json_path}
                  secondaryAction={
                    <IconButton edge="end" aria-label="add"></IconButton>
                  }
                  sx={{ paddingLeft: "10px" }}
                >
                  <Tooltip
                    title={field.field_mapping.type}
                    arrow
                    placement="left"
                  >
                    <Chip
                      label={fieldTypeLabel(field.field_mapping)}
                      size="small"
                      sx={{
                        marginRight: "10px",
                        borderRadius: "3px",
                        fontSize: "0.6rem",
                      }}
                    />
                  </Tooltip>
                  <ListItemText primary={field.json_path} />
                </ListItem>
              );
            })}
          </List>
        )}
      </Box>
    </IndexBarWrapper>
  );
}
