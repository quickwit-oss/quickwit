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
import SoehneMonoKraftigWoff2 from "./../assets/fonts/soehne-mono-web-kraftig.woff2";
import SoehneMonoDreiviertelfettWoff2 from "./../assets/fonts/soehne-mono-web-dreiviertelfett.woff2";
import SoehneHalbfettWoff2 from "./../assets/fonts/soehne-web-halbfett.woff2";
import SoehneBuchWoff2 from "./../assets/fonts/soehne-web-buch.woff2";

export const QUICKWIT_BLUE = '#004BD9';
export const QUICKWIT_RED = '#FF0026';
export const QUICKWIT_GREEN = '#00D588';
export const QUICKWIT_GREY = '#CBD1DE';
export const QUICKWIT_INTERMEDIATE_GREY = 'rgba(203,209,222,0.5)';
export const QUICKWIT_LIGHT_GREY = '#F8F9FB';
export const QUICKWIT_BLACK = '#1F232A';


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
    fontFamily: 'SoehneMono, Arial',
    body1: {
      fontSize: '0.8rem',
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


export const EditorTheme = {
  base: 'vs',
  inherit: true,
  rules: [
    { token: 'comment', foreground: '#1F232A', fontStyle: 'italic' },
    { token: 'keyword', foreground: QUICKWIT_BLUE }
  ],
  colors: {
    'editor.comment.foreground': '#CBD1DE',
    'editor.foreground': '#000000',
    'editor.background': QUICKWIT_LIGHT_GREY,
    'editorLineNumber.foreground': 'black',
    'editor.lineHighlightBackground': '#DFE0E1',
  },
};