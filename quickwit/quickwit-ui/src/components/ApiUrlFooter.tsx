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

import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import { Box, Button, styled, Typography } from "@mui/material";
import { QUICKWIT_LIGHT_GREY } from "../utils/theme";

const Footer = styled(Box)`
  display: flex;
  height: 25px;
  padding: 0px 5px;
  position: absolute;
  bottom: 0px;
  font-size: 0.9em;
  background-color: ${QUICKWIT_LIGHT_GREY};
  opacity: 0.7;
`;

export default function ApiUrlFooter(url: string) {
  const urlMaxLength = 80;
  const origin =
    // @ts-ignore
    process.env.NODE_ENV === "development"
      ? "http://localhost:7280"
      : window.location.origin;
  const completeUrl = `${origin}/${url}`;
  const isTooLong = completeUrl.length > urlMaxLength;
  // TODO show generated aggregation
  return (
    <Footer>
      <Typography sx={{ padding: "4px 5px", fontSize: "0.95em" }}>
        API URL:
      </Typography>
      <Button
        sx={{
          fontSize: "0.93em",
          textTransform: "inherit",
          whiteSpace: "nowrap",
          overflow: "hidden",
          textOverflow: "clip",
        }}
        onClick={() => {
          if (window.isSecureContext) {
            navigator.clipboard.writeText(completeUrl);
          } else {
            window.open(completeUrl, "_blank");
          }
        }}
        endIcon={<ContentCopyIcon />}
        size="small"
      >
        {completeUrl.substring(0, urlMaxLength)}
        {isTooLong && "..."}
      </Button>
    </Footer>
  );
}
