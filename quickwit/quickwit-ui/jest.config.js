module.exports = {
  setupFiles: [
    "react-app-polyfill/jsdom", // polyfill jsdom api (such as fetch)
    "<rootDir>/jest/setup.js", // polyfill textEncode
  ],

  setupFilesAfterEnv: ["@testing-library/jest-dom"],

  testEnvironment: "jsdom",

  transform: {
    // transform js file (typescript and es6 import)
    "^.+\\.(js|jsx|mjs|cjs|ts|tsx)$": [
      "babel-jest",
      {
        presets: [["babel-preset-react-app", { runtime: "automatic" }]],
        plugins: [
          [
            "@dr.pogodin/babel-plugin-transform-assets",
            { extensions: ["svg", "woff2"] },
          ],
        ],
        babelrc: false,
        configFile: false,
      },
    ],
  },

  moduleNameMapper: {
    "@monaco-editor/react": "<rootDir>/mocks/monacoMock.js",
    "swagger-ui-react": "<rootDir>/mocks/swaggerUIMock.js",
    "@mui/x-charts": "<rootDir>/mocks/x-charts.js",
  },

  resetMocks: true,
};
