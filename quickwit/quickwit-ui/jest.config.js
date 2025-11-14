module.exports = {
  setupFiles: ["react-app-polyfill/jsdom"], // polyfill jsdom api (such as fetch)

  setupFilesAfterEnv: ["@testing-library/jest-dom"],

  testEnvironment: "jsdom",

  transform: {
    // transform js file (typescript and es6 import)
    "^.+\\.(js|jsx|mjs|cjs|ts|tsx)$": [
      "babel-jest",
      {
        presets: [["babel-preset-react-app", { runtime: "automatic" }]],
        babelrc: false,
        configFile: false,
      },
    ],

    // transform asset files
    "^(?!.*\\.(js|jsx|mjs|cjs|ts|tsx|css|json)$)": "jest-transform-stub",
  },

  moduleNameMapper: {
    "monaco-editor": "<rootDir>/mocks/monacoMock.js",
    "swagger-ui-react": "<rootDir>/mocks/swaggerUIMock.js",
    "@mui/x-charts": "<rootDir>/mocks/x-charts.js",
  },

  resetMocks: true,
};
