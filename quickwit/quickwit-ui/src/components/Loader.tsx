// Copyright (C) 2022 Quickwit, Inc.
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

import { Box, styled, keyframes } from '@mui/material';
import { ReactComponent as Logo } from '../assets/img/logo.svg';

const spin = keyframes`
from {
  transform: rotate(0deg);
}
to {
  transform: rotate(360deg);
}
`

const SpinningLogo = styled(Logo)`
height: 10vmin;
pointer-events: none;
fill: #CBD1DD;
animation: ${spin} infinite 5s linear;
`

export default function Loader() {
  return <Box
    display="flex"
    justifyContent="center"
    alignItems="center"
    minHeight="40vh"
    >
    <SpinningLogo></SpinningLogo>
  </Box>
}
