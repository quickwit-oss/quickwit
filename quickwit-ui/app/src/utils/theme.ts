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

import { createTheme } from "@mui/material";
import SoehneMonoWebWoff2 from "./../assets/fonts/soehne-mono-web-kraftig.woff2";

// Update the Typography's var@iant prop options
declare module '@mui/material/Typography' {
  interface TypographyPropsVariantOverrides {
    fontSize: true;
    poster: true;
    h3: false;
  }
}

declare module '@mui/material/styles' {
  interface Theme {
    status: {
      danger: React.CSSProperties['color'];
    };
  }

  interface PaletteOptions {
    neutral: PaletteOptions['primary'];
  }

  interface Palette {
    primary: Palette['primary'];
    secondary: Palette['secondary'];
    text: Palette['text'];
    neutral: Palette['primary'];
  }
}

export const theme = createTheme({
  palette: {
    primary: {
      main: "#000000",
      contrastText: '#ffffff'
    },
    secondary: {
      main: '#000000',
    },
    text: {
      primary: '#000000',
    },
    neutral: {
      main: '#F8F9FB',
      contrastText: '#000000',
    },
  },
  typography: {
    fontSize: 12,
    fontFamily: 'Soehne, Arial',
  },
  components: {
    MuiCssBaseline: {
      styleOverrides: `
        @font-face {
          font-family: 'Soehne';
          font-style: normal;
          font-display: swap;
          font-weight: 400;
          src: local('SoehneMonoWeb'), local('SoehneMonoWeb-Dreiviertelfett'), url(${SoehneMonoWebWoff2}) format('woff2');
        }
      `,
    },
  },
});
