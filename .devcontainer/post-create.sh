#!/bin/bash

# Define success and error color codes
SUCCESS_COLOR="\e[32m"
ERROR_COLOR="\e[31m"
RESET_COLOR="\e[0m"

# Define success tracking variables
rustupToolchainNightlyInstalled=false
cmakeInstalled=false


# Define installation functions

#Installing manually for now until we figure out why "ghcr.io/devcontainers-community/features/cmake": {} is not working
install_cmake() {
    echo -e "Installing CMake..."
    sudo apt-get update
    sudo apt-get install -y cmake > /dev/null 2>&1
    if [[ "$(cmake --version)" =~ "cmake version" ]]; then
        echo -e "${SUCCESS_COLOR}CMake installed successfully.${RESET_COLOR}"
        cmakeInstalled=true
    else
        echo -e "${ERROR_COLOR}CMake installation failed. Please install it manually.${RESET_COLOR}"
    fi
}

install_rustup_toolchain_nightly() {
    echo -e "Installing Rustup nightly toolchain..."
    rustup toolchain install nightly > /dev/null 2>&1
    rustup component add rustfmt --toolchain nightly > /dev/null 2>&1
    if [[ "$(rustup toolchain list)" =~ "nightly" && "$(rustup component list --toolchain nightly | grep rustfmt)" =~ "installed" ]]; then
        echo -e "${SUCCESS_COLOR}Rustup nightly toolchain and rustfmt installed successfully.${RESET_COLOR}"
        rustupToolchainNightlyInstalled=true
    else
        echo -e "${ERROR_COLOR}Rustup nightly toolchain and/or rustfmt installation failed. Please install them manually.${RESET_COLOR}"
    fi
}

# Install tools
install_cmake
install_rustup_toolchain_nightly

# Copy our custom welcome message to replace the default github welcome message
sudo cp .devcontainer/welcome.txt /usr/local/etc/vscode-dev-containers/first-run-notice.txt


# Check the success tracking variables
if $rustupToolchainNightlyInstalled && $cmakeInstalled; then
    echo -e "${SUCCESS_COLOR}All tools installed successfully.${RESET_COLOR}"
else
    echo -e "${ERROR_COLOR}One or more tools failed to install. Please check the output for errors and install the failed tools manually.${RESET_COLOR}"
fi
