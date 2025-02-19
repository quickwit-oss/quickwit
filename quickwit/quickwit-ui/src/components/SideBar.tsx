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

import * as React from 'react';
import List from '@mui/material/List';
import ListItem from '@mui/material/ListItem';
import ListItemIcon from '@mui/material/ListItemIcon';
import ListItemText from '@mui/material/ListItemText';
import {
  Link as RouterLink,
  LinkProps as RouterLinkProps,
} from 'react-router-dom';
import { ListSubheader, styled, Typography } from '@mui/material';
import { CodeSSlash } from "@styled-icons/remix-line/CodeSSlash"
import { GroupWork } from '@styled-icons/material-outlined/GroupWork';
import { Database } from '@styled-icons/feather/Database';
import { Settings } from '@styled-icons/feather/Settings';
import { useLocalStorage } from '../providers/LocalStorageProvider';
import { toUrlSearchRequestParams } from '../utils/urls';
import { APP_BAR_HEIGHT_PX } from './LayoutUtils';

interface ListItemLinkProps {
  icon?: React.ReactElement;
  primary: React.ReactElement;
  to: string;
}

function ListItemLink(props: ListItemLinkProps) {
  const { icon, primary, to } = props;

  const renderLink = React.useMemo(
    () =>
      React.forwardRef<HTMLAnchorElement, Omit<RouterLinkProps, 'to'>>(function Link(
        itemProps,
        ref,
      ) {
        return <RouterLink to={to} ref={ref} {...itemProps} role={undefined} />;
      }),
    [to],
  );

  return (
    <ListItem button component={renderLink}>
      {icon ? <ListItemIcon sx={{ minWidth: "40px" }}>{icon}</ListItemIcon> : null}
      <ListItemText primary={primary} />
    </ListItem>
  );
}

const SideBarWrapper = styled('div')({
  display: 'flex',
  marginTop: `${APP_BAR_HEIGHT_PX}`,
  height: `calc(100% - ${APP_BAR_HEIGHT_PX})`,
  flex: '0 0 180px',
  flexDirection: 'column',
  borderRight: '1px solid rgba(0, 0, 0, 0.12)',
});

const SideBar = () => {
  const lastSearchRequest = useLocalStorage().lastSearchRequest;
  let searchUrl = '/search';
  if (lastSearchRequest.indexId || lastSearchRequest.query) {
    searchUrl = '/search?' + toUrlSearchRequestParams(lastSearchRequest).toString();
  }
  return (
    <SideBarWrapper sx={{ px: 0, py: 2 }}>
      <List dense={ true } sx={{ py: 0 }}>
        <ListSubheader sx={{lineHeight: '25px'}}>
          <Typography variant="body1">Discover</Typography>
        </ListSubheader>
        <ListItemLink to={searchUrl} primary={<Typography variant="body1">Query editor</Typography>} icon={<CodeSSlash size="18px" />} />
        <ListSubheader sx={{lineHeight: '25px', paddingTop: '10px'}}>
          <Typography variant="body1">Admin</Typography>
        </ListSubheader>
          <ListItemLink to="/indexes" primary={<Typography variant="body1">Indexes</Typography>} icon={<Database size="18px" />} />
          <ListItemLink to="/cluster" primary={<Typography variant="body1">Cluster</Typography>} icon={<GroupWork size="18px" />} />
          <ListItemLink to="/node-info" primary={<Typography variant="body1">Node info</Typography>} icon={<Settings size="18px" />} />
          <ListItemLink to="/api-playground" primary={<Typography variant="body1">API </Typography>} icon={<CodeSSlash size="18px" />} />
      </List>
    </SideBarWrapper>
  );
};

export default SideBar;
