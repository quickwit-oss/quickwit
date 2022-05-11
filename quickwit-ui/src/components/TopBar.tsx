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

import AppBar from '@mui/material/AppBar';
import Toolbar from '@mui/material/Toolbar';
import GitHubIcon from '@mui/icons-material/GitHub';
import { Box, IconButton, Link, styled, SvgIcon } from '@mui/material';
import { Discord } from '@styled-icons/fa-brands/Discord';
import { ReactComponent as Logo } from '../assets/img/quickwit-logo.svg';

const StyledAppBar = styled(AppBar)(({ theme })=>({
  zIndex: theme.zIndex.drawer + 1,
}));

// Update the Button's color prop options
declare module '@mui/material/AppBar' {
  interface AppBarPropsColorOverrides {
    neutral: true;
  }
}


const TopBar = () => {
  return (
    <StyledAppBar position="fixed" elevation={0} color="neutral">
      <Toolbar variant="dense">
        <Box sx={{ flexGrow: 1, p: 0, m: 0, display: 'flex', alignItems: 'center' }}>
          <Logo height='25px'></Logo>
        </Box>
        <Link href="https://quickwit.io/docs" target="_blank" sx={{ px: 2 }}>
            Docs
        </Link>
        <Link href="https://discord.gg/rpRRTezWhW" target="_blank">
          <IconButton size="large">
            <SvgIcon>
              <Discord />
            </SvgIcon>
          </IconButton>
        </Link>
        <Link href="https://github.com/quickwit-inc/quickwit" target="_blank">
          <IconButton size="large">
            <GitHubIcon />
          </IconButton>
        </Link>
      </Toolbar>
    </StyledAppBar>
  );
};
export default TopBar;
