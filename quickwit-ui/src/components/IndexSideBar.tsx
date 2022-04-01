// Copyright (C) 2021 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

import { Autocomplete, Chip, CircularProgress, IconButton, List, ListItem, ListItemText, TextField, Typography } from '@mui/material';
import { Box } from '@mui/system';
import React, { useEffect, useMemo, useState } from 'react';
import styled from '@emotion/styled';
import { FieldMapping, getAllFields, IndexMetadata } from '../utils/models';
import { ChevronRight, KeyboardArrowDown } from '@mui/icons-material';
import { Client } from '../services/client';

const IndexBarWrapper = styled('div')({
  display: 'flex',
  height: '100%',
  flex: '0 0 260px',
  flexDirection: 'column',
  borderRight: '1px solid rgba(0, 0, 0, 0.12)',
});

function IndexAutocomplete(props: IndexMetadataProps) {
  const [open, setOpen] = React.useState(false);
  const [options, setOptions] = React.useState<readonly IndexMetadata[]>([]);
  const [value, setValue] = React.useState<IndexMetadata | null>(null);
  const loading = open && options.length <= 1;
  const quickwitClient = useMemo(() => new Client(), []);

  useEffect(() => {
    if (!loading) {
      return;
    }
    quickwitClient.listIndexes().then(
      (indexesMetadata) => {
        setOptions([...indexesMetadata]);
      },
      (error) => {
        console.log("Index autocomplete error", error);
      }
    );
  }, [quickwitClient, loading]);

  useEffect(() => {
    if (!open) {
      if (props.indexMetadata !== null && options.length === 0) {
        setOptions([props.indexMetadata]);
      }
    }
  }, [open, props.indexMetadata, options.length]);

  useEffect(() => {
    if (props.indexMetadata !== null) {
      setValue(props.indexMetadata);
    } else {
      setValue(null);
    }
  }, [props.indexMetadata]);

  return (
    <Autocomplete
      size="small"
      sx={{ width: 210 }}
      open={open}
      value={value}
      onChange={(_, updatedValue) => {
        if (updatedValue === null) {
          setValue(null);
        } else {
          setValue(updatedValue);
        }
        if (updatedValue == null || updatedValue.index_id == null) {
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
      }}
      isOptionEqualToValue={(option, value) => option.index_id === value.index_id}
      getOptionLabel={(option) => option.index_id}
      options={options}
      loading={loading}
      renderInput={(params) => (
        <TextField
          {...params}
          placeholder='Select an index'
          InputProps={{
            ...params.InputProps,
            endAdornment: (
              <React.Fragment>
                {loading ? <CircularProgress color="inherit" size={20} /> : null}
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
  indexMetadata: null | IndexMetadata,
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
  const fields = (props.indexMetadata === null) ? [] : getAllFields(props.indexMetadata.doc_mapping);
  return (
    <IndexBarWrapper>
      <Box sx={{ px: 3, py: 2}}>
        <Typography variant='body1' mb={1}>
          Index ID
        </Typography>
        <IndexAutocomplete { ...props }/>
      </Box>
      <Box sx={{ paddingLeft: "10px"}}>
        <IconButton
            aria-label="expand row"
            size="small"
            onClick={() => setOpen(!open)}
          >
            {open ? <KeyboardArrowDown /> : <ChevronRight />}
        </IconButton>
        Fields
        { open && <List dense={true} sx={{paddingTop: '0'}}>
          { fields.map(function(field) {
            return <ListItem
              key={ field.name }
              secondaryAction={
                <IconButton edge="end" aria-label="add"></IconButton>
              }
              sx={{paddingLeft: '10px'}}
            >
              <Chip label={fieldTypeLabel(field)} size="small" sx={{marginRight: '10px', borderRadius: '3px', fontSize: '0.6rem'}}/>
              <ListItemText primary={ field.name }/>
            </ListItem>
          })}
        </List>
        }
      </Box>
    </IndexBarWrapper>
  );
}
