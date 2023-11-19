#!/bin/bash

# Define success tracking variables
cmakeInstalled=false
dockerInstalled=false
dockerComposeInstalled=false
nodeInstalled=false
yarnInstalled=false
awslocalInstalled=false
protocInstalled=false

# Define installation functions
install_cmake() {
    echo "Installing CMake..."
    sudo apt-get update
    sudo apt-get install -y cmake
    if [[ "$(cmake --version)" =~ "cmake version" ]]; then
        echo "CMake installed successfully."
        cmakeInstalled=true
    else
        echo "CMake installation failed. Please install it manually."
    fi
}

install_node_yarn() {
    echo "Installing Node v18 and Yarn..."
    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.38.0/install.sh | bash
    source ~/.bashrc
    nvm install 18
    nvm use 18
    npm install -g yarn
    if [[ "$(node --version)" =~ "v18." && "$(yarn --version)" =~ "." ]]; then
        echo "Node v18 and Yarn installed successfully."
        nodeInstalled=true
        yarnInstalled=true
    else
        echo "Node v18 and/or Yarn installation failed. Please install them manually."
    fi
}

configure_python_environment() {
    if command -v python3 &> /dev/null; then
        echo "Python 3 is installed"
        # Create a symbolic link from python to python3
        sudo ln -s /usr/bin/python3 /usr/bin/python
    else
        echo "Python 3 is not installed. Please install it manually."
    fi
}

install_awslocal() {
    echo "Setting up python environment"
    configure_python_environment

    echo "Installing awslocal..."
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
    unzip awscliv2.zip
    sudo ./aws/install

    # Clean up
    rm -f awscliv2.zip
    # Installation files are stored in /usr/local/aws-cli, so we need to remove them
    rm -rf aws
    
    if [[ "$(aws --version)" =~ "aws-cli" ]]; then
        echo "awslocal installed successfully."
        awslocalInstalled=true
    else
        echo "awslocal installation failed. Please install it manually."
    fi
}

install_protoc() {
    echo "Installing protoc..."
    PB_REL="https://github.com/protocolbuffers/protobuf/releases"
    curl -LO $PB_REL/download/v3.15.8/protoc-3.15.8-linux-x86_64.zip
    unzip protoc-3.15.8-linux-x86_64.zip -d $HOME/.local
    export PATH="$PATH:$HOME/.local/bin"
    if [[ "$(protoc --version)" =~ "libprotoc 3.15.8" ]]; then
        echo "protoc installed successfully."
        protocInstalled=true
    else
        echo "protoc installation failed. Please install it manually."
    fi
    
    # Clean up
    rm -f protoc-3.15.8-linux-x86_64.zip
}

install_rustup_toolchain_nightly() {
    echo "Installing Rustup nightly toolchain..."
    rustup toolchain install nightly
    rustup component add rustfmt --toolchain nightly
    if [[ "$(rustup toolchain list)" =~ "nightly" && "$(rustup component list --toolchain nightly | grep rustfmt)" =~ "installed" ]]; then
        echo "Rustup nightly toolchain and rustfmt installed successfully."
    else
        echo "Rustup nightly toolchain and/or rustfmt installation failed. Please install them manually."
    fi
}
# Call the functions
install_cmake
install_node_yarn
install_awslocal
install_protoc
install_rustup_toolchain_nightly

