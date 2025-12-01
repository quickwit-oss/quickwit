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

import { createTheme } from "@mui/material";
import SoehneMonoDreiviertelfettWoff2 from "./../assets/fonts/soehne-mono-web-dreiviertelfett.woff2";
import SoehneMonoKraftigWoff2 from "./../assets/fonts/soehne-mono-web-kraftig.woff2";
import SoehneBuchWoff2 from "./../assets/fonts/soehne-web-buch.woff2";
import SoehneHalbfettWoff2 from "./../assets/fonts/soehne-web-halbfett.woff2";

export const QUICKWIT_BLUE = "#004BD9";
export const QUICKWIT_RED = "#FF0026";
export const QUICKWIT_GREEN = "#00D588";
export const QUICKWIT_GREY = "#CBD1DE";
export const QUICKWIT_INTERMEDIATE_GREY = "rgba(203,209,222,0.5)";
export const QUICKWIT_LIGHT_GREY = "#F8F9FB";
export const QUICKWIT_BLACK = "#1F232A";

// Update the Typography's var@iant prop options
declare module "@mui/material/Typography" {
  interface TypographyPropsVariantOverrides {
    fontSize: true;
    poster: true;
    h3: false;
  }
}

declare module "@mui/material/styles" {
  interface Theme {
    status: {
      danger: React.CSSProperties["color"];
    };
  }

  interface PaletteOptions {
    neutral: PaletteOptions["primary"];
  }

  interface Palette {
    primary: Palette["primary"];
    secondary: Palette["secondary"];
    text: Palette["text"];
    neutral: Palette["primary"];
  }
}

export const theme = createTheme({
  palette: {
    primary: {
      main: "#000000",
      contrastText: "#ffffff",
    },
    secondary: {
      main: "#000000",
    },
    text: {
      primary: "#000000",
    },
    neutral: {
      main: "#F8F9FB",
      contrastText: "#000000",
    },
  },
  typography: {
    fontSize: 12,
    fontFamily: "SoehneMono, Arial",
    body1: {
      fontSize: "0.8rem",
    },
  },
  components: {
    MuiCssBaseline: {
      styleOverrides: `
        @font-face {
          font-family: 'SoehneMono';
          font-style: normal;
          font-display: swap;
          font-weight: 500;
          src: local('SoehneMonoKraftig'), local('SoehneMonoKraftig'), url(${SoehneMonoKraftigWoff2}) format('woff2');
        }
        @font-face {
          font-family: 'SoehneMono';
          font-style: normal;
          font-display: swap;
          font-weight: 700;
          src: local('SoehneMonoDreiviertelfett'), local('SoehneMonoDreiviertelfett'), url(${SoehneMonoDreiviertelfettWoff2}) format('woff2');
        }
        @font-face {
          font-family: 'Soehne';
          font-style: bold;
          font-display: swap;
          font-weight: 600;
          src: local('SoehneHalbfett'), local('SoehneHalbfett'), url(${SoehneHalbfettWoff2}) format('woff2');
        }
        @font-face {
          font-family: 'Soehne';
          font-style: normal;
          font-display: swap;
          font-weight: 300;
          src: local('SoehneBuch'), local('SoehneBuch'), url(${SoehneBuchWoff2}) format('woff2');
        }
      `,
    },
  },
});

export const EDITOR_THEME = {
  base: "vs" as const,
  inherit: true,
  rules: [
    { token: "comment", foreground: "#1F232A", fontStyle: "italic" },
    { token: "keyword", foreground: QUICKWIT_BLUE },
  ],
  colors: {
    "editor.comment.foreground": "#CBD1DE",
    "editor.foreground": "#000000",
    "editor.background": QUICKWIT_LIGHT_GREY,
    "editorLineNumber.foreground": "black",
    "editor.lineHighlightBackground": "#DFE0E1",
  },
};
