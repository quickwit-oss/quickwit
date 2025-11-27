# quickwit-ui



## Prerequisites

`node` and `yarn` need to be installed on your system.
The project then relies on misc nodejs tools that can be installed locally by 
running `yarn`.

## Available Scripts


In the project directory, you can run:


### `yarn start`

Runs the app in the development mode.\
Open [http://localhost:3000](http://localhost:3000) to view it in the browser.

The page will reload if you make edits.\
You will also see any lint errors in the console.

### `yarn test`

Launches the test runner.

### `yarn e2e-test`

Launches the e2e test runner with [cypress](https://www.cypress.io/). To make them work, you need to start a
searcher beforehand with `cargo r run --service searcher --config config/quickwit.yaml`.

### `yarn format`

Re-writes files with the correct formatting if needed.\
You might want to configure your IDE to do that [automatically](https://biomejs.dev/guides/editors/first-party-extensions/).

### `yarn build`

Builds the app for production to the `build` folder.\
It correctly bundles React in production mode and optimizes the build for the best performance.

The build is minified and the filenames include the hashes.\
Your app is ready to be deployed!
