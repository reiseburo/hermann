stage (‘Build’) {
 node {
 // Checkout
 checkout scm
 // install required bundles
 sh ‘bundle install’
 // build and run tests with coverage
 sh ‘bundle exec rake build spec’
 // Archive the built artifacts
 archive (includes: ‘pkg/*.gem’)
 }
 }
