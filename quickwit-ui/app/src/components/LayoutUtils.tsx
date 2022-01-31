import { Breadcrumbs } from "@mui/material";
import { styled, Box } from "@mui/system";

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
padding-bottom: 16px;
`
