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

import * as React from 'react';
import { styled } from '@mui/system';
import List from '@mui/material/List';
import ListItem from '@mui/material/ListItem';
import ListItemIcon from '@mui/material/ListItemIcon';
import ListItemText from '@mui/material/ListItemText';
import {
  Link as RouterLink,
  LinkProps as RouterLinkProps,
} from 'react-router-dom';
import { ListSubheader, Typography } from '@mui/material';
import { CodeSSlash } from "@styled-icons/remix-line/CodeSSlash"
import { GroupWork } from '@styled-icons/material-outlined/GroupWork';
import { Database } from '@styled-icons/feather/Database';
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
          <ListItemLink to="/cluster/members" primary={<Typography variant="body1">Cluster</Typography>} icon={<GroupWork size="18px" />} />
      </List>
    </SideBarWrapper>
  );
};

export default SideBar;
