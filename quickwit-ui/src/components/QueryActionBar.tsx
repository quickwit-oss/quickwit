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

import { Button } from "@mui/material";
import { Box } from "@mui/system";
import { TimeRangeSelect } from './TimeRangeSelect';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import { SearchComponentProps } from "../utils/SearchComponentProps";

export function QueryEditorActionBar(props: SearchComponentProps) {
  return (
    <Box sx={{ display: 'flex'}}>
      <Box sx={{ flexGrow: 1 }}>
        <Button
          onClick={() => props.runSearch(props.searchRequest)}
          variant="contained"
          startIcon={<PlayArrowIcon />}
          disableElevation
          sx={{ flexGrow: 1}}
          disabled={props.queryRunning || props.searchRequest.indexId === null}>
          Run
        </Button>
      </Box>
      { props.index?.metadata.indexing_settings.timestamp_field && <TimeRangeSelect 
        { ...props } /> 
      }
    </Box>
  )
}