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

import { Box, styled, Typography, Button } from '@mui/material';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import { QUICKWIT_LIGHT_GREY } from '../utils/theme';

const Footer = styled(Box)`
display: flex;
height: 25px;
padding: 0px 5px;
position: absolute;
bottom: 0px;
font-size: 0.90em;
background-color: ${QUICKWIT_LIGHT_GREY};
opacity: 0.7;
`

export default function ApiUrlFooter(url: string) {
  const urlMaxLength = 80;
  const origin = process.env.NODE_ENV === 'development' ? 'http://localhost:7280' : window.location.origin;
  const completeUrl = `${origin}/${url}`;
  const isTooLong = completeUrl.length > urlMaxLength;
  return <Footer>
    <Typography sx={{ padding: '4px 5px', fontSize: '0.95em'}}>
      API URL: 
    </Typography>
    <Button
      sx={{ fontSize: '0.93em', textTransform: 'inherit', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'clip' }}
      onClick={() => {navigator.clipboard.writeText(completeUrl)}}
      endIcon={<ContentCopyIcon />}
      size="small">
        {completeUrl.substring(0, urlMaxLength)}{isTooLong && "..."}
    </Button>
  </Footer>
}
