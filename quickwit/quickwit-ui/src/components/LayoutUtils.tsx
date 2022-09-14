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

import { Box, Breadcrumbs, styled } from "@mui/material";

export const APP_BAR_HEIGHT_PX = '48px';
export const ViewUnderAppBarBox = styled(Box)`
display: flex;
flex-direction: column;
margin-top: ${APP_BAR_HEIGHT_PX};
height: calc(100% - ${APP_BAR_HEIGHT_PX});
width: 100%;
`;
export const FullBoxContainer = styled(Box)`
display: flex;
flex-direction: column;
height: 100%;
width: 100%;
padding: 16px 24px;
`;
export const QBreadcrumbs = styled(Breadcrumbs)`
padding-bottom: 8px;
`
