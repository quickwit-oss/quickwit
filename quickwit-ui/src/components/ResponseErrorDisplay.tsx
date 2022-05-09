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

import { Box } from '@mui/material';
import { ResponseError } from '../utils/models';
import SentimentVeryDissatisfiedIcon from '@mui/icons-material/SentimentVeryDissatisfied';

export default function ErrorResponseDisplay(error: ResponseError) {
  return <Box sx={{ pt: 2, display: 'flex', flexDirection: 'column', alignItems: 'center' }} >
    <SentimentVeryDissatisfiedIcon sx={{ fontSize: 60 }} />
      <Box sx={{fontSize: 16, pt: 2, }}>
        {error.status && <span>Status: {error.status}</span>}
      </Box>
      <Box sx={{ fontSize: 14, pt: 1, alignItems: 'center'}}>
        Error: {error.message}
      </Box>
    </Box>
}
