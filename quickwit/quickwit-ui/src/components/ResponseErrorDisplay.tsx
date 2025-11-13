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

import SentimentVeryDissatisfiedIcon from "@mui/icons-material/SentimentVeryDissatisfied";
import { Box } from "@mui/material";
import { ResponseError } from "../utils/models";

function renderMessage(error: ResponseError) {
  if (
    error.message !== null &&
    error.message.includes("No search node available.")
  ) {
    return (
      <Box sx={{ fontSize: 16, pt: 2 }}>
        Your cluster does not contain any search node. You need at least one
        search node.
      </Box>
    );
  } else {
    return (
      <>
        <Box sx={{ fontSize: 16, pt: 2 }}>
          {error.status && <span>Status: {error.status}</span>}
        </Box>
        <Box sx={{ fontSize: 14, pt: 1, alignItems: "center" }}>
          Error: {error.message}
        </Box>
      </>
    );
  }
}

export default function ErrorResponseDisplay(error: ResponseError) {
  return (
    <Box
      sx={{
        pt: 2,
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
      }}
    >
      <SentimentVeryDissatisfiedIcon sx={{ fontSize: 60 }} />
      {renderMessage(error)}
    </Box>
  );
}
