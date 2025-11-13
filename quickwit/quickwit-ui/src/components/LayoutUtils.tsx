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

import { Box, Breadcrumbs, styled } from "@mui/material";

export const APP_BAR_HEIGHT_PX = "48px";
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
`;
