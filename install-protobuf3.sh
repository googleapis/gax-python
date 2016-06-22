#!/bin/bash

export PATH="$BREW_HOME/bin:$PATH"

echo "Testing for protoc command"
command -v protoc || {
  echo "Testing for brew command"
  command -v brew || {
    echo "Installing brew"
    # install brew in headless mode to avoid user prompt requiring input
    </dev/null ruby -e \
      "$(curl -fsSL https://raw.githubusercontent.com/Linuxbrew/install/master/install)"
  }

  echo "Installing proto3 via brew"
  brew install --devel protobuf
}

# ensure protoc is up-to-date in our cache
echo "Upgrading brew packages"
brew update
brew upgrade
