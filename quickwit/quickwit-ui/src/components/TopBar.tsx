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

import GitHubIcon from "@mui/icons-material/GitHub";
import {
  Box,
  IconButton,
  Link,
  SvgIcon,
  styled,
  Tooltip,
  Typography,
} from "@mui/material";
import AppBar from "@mui/material/AppBar";
import Toolbar from "@mui/material/Toolbar";
import { Discord } from "@styled-icons/fa-brands/Discord";
import { useEffect, useMemo, useState } from "react";
import quickwitLogoUrl from "../assets/img/quickwit-logo-with-title.svg";
import { Client } from "../services/client";

const Logo = (props: React.ComponentProps<"img">) => (
  <img {...props} src={quickwitLogoUrl} alt="quickwit logo" />
);

const StyledAppBar = styled(AppBar)(({ theme }) => ({
  zIndex: theme.zIndex.drawer + 1,
}));

// Update the Button's color prop options
declare module "@mui/material/AppBar" {
  interface AppBarPropsColorOverrides {
    neutral: true;
  }
}

const TopBar = () => {
  const [clusterId, setClusterId] = useState<string>("");
  const quickwitClient = useMemo(() => new Client(), []);

  useEffect(() => {
    quickwitClient.cluster().then((cluster) => {
      setClusterId(cluster.cluster_id);
    });
  }, [quickwitClient]);

  return (
    <StyledAppBar position="fixed" elevation={0} color="neutral">
      <Toolbar variant="dense">
        <Box
          sx={{
            flexGrow: 1,
            p: 0,
            m: 0,
            display: "flex",
            alignItems: "center",
          }}
        >
          <Logo height="25px"></Logo>
          <Tooltip title="Cluster ID" placement="right">
            <Typography mx={2}>{clusterId}</Typography>
          </Tooltip>
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
