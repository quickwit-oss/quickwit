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

import { Box, keyframes, styled } from "@mui/material";
import loadinIcongUrl from "../assets/img/quickwit-logo-monochrome.svg";

const spin = keyframes`
from {
  transform: rotate(0deg);
}
to {
  transform: rotate(360deg);
}
`;

const LoadingIcon = (props: React.ComponentProps<"img">) => (
  <img {...props} src={loadinIcongUrl} alt="loading icon" />
);

const SpinningLoadingIcon = styled(LoadingIcon)`
  height: 10vmin;
  pointer-events: none;
  fill: #cbd1dd;
  animation: ${spin} infinite 5s linear;
`;

export default function Loader() {
  return (
    <Box
      display="flex"
      justifyContent="center"
      alignItems="center"
      minHeight="40vh"
    >
      <SpinningLoadingIcon></SpinningLoadingIcon>
    </Box>
  );
}
