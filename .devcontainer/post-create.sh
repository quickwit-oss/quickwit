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

install_docker() {
    echo "Installing Docker..."
    curl -fsSL https://get.docker.com -o get-docker.sh
    sudo sh get-docker.sh
    if [[ "$(docker --version)" =~ "Docker version" ]]; then
        echo "Docker installed successfully."
        dockerInstalled=true
    else
        echo "Docker installation failed. Please install it manually."
    
    # Clean up
    rm -f get-docker.sh
    fi
    
    echo "Installing Docker Compose..."
    sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
    if [[ "$(docker-compose --version)" =~ "docker-compose version" ]]; then
        echo "Docker Compose installed successfully."
        dockerComposeInstalled=true
    else
        echo "Docker Compose installation failed. Please install it manually."
    fi
}

install_node_yarn() {
    echo "Installing Node v18 and Yarn..."
    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.38.0/install.sh | bash
    source ~/.bashrc
    nvm install 18
    npm install -g yarn
    if [[ "$(node --version)" =~ "v18." && "$(yarn --version)" =~ "." ]]; then
        echo "Node v18 and Yarn installed successfully."
        nodeInstalled=true
        yarnInstalled=true
    else
        echo "Node v18 and/or Yarn installation failed. Please install them manually."
    fi
}

install_awslocal() {
    echo "Installing awslocal..."
    pip install awscli-local
    if [[ "$(awslocal --version)" =~ "aws-cli" ]]; then
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

    # Clean up
    rm -f protoc-3.15.8-linux-x86_64.zip
    fi
}

install_rustup_toolchain_nightly() {
    echo "Installing Rustup nightly toolchain..."
    rustup toolchain install nightly
    if [[ "$(rustup toolchain list)" =~ "nightly" ]]; then
        echo "Rustup nightly toolchain installed successfully."
    else
        echo "Rustup nightly toolchain installation failed. Please install it manually."
    fi
}

# Call the functions
install_cmake
install_docker
install_node_yarn
install_awslocal
install_protoc
install_rustup_toolchain_nightly

# Check the success tracking variables
if $cmakeInstalled && $dockerInstalled && $dockerComposeInstalled && $nodeInstalled && $yarnInstalled && $awslocalInstalled && $protocInstalled; then
    echo "All tools installed successfully."
    echo "Useful commands:"
    echo "make test-all - starts necessary Docker services and runs all tests."
    echo "make -k test-all docker-compose-down - the same as above, but tears down the Docker services after running all the tests."
    echo "make fmt - runs formatter, this command requires the nightly toolchain to be installed by running rustup toolchain install nightly."
    echo "make fix - runs formatter and clippy checks."
    echo "make typos - runs the spellcheck tool over the codebase. (Install by running cargo install typos)"
    echo "make build-docs - builds docs."
    echo "make docker-compose-up - starts Docker services."
    echo "make docker-compose-down - stops Docker services."
    echo "make docker-compose-logs - shows Docker logs."
else
    echo "One or more tools failed to install. Please check the output for errors and install the failed tools manually."
fi
