#!/bin/bash -xe

source $HOME/.rvm/scripts/rvm

rvm use `cat .ruby-ci-version`@`cat .ruby-ci-gemset` --create

gem install bundler
bundle install

bundle exec rake -v --trace "tarball_artifactory:upload"
