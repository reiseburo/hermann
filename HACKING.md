# Hacking on Hermann


### Integration Testing

* Download Kafka
* Start Zookeeper
 * set port 2181
* Start Kafka
  * Set properties file ```zookeeper.connect=localhost:2181```
* ```bundle exec jruby -S rspec spec/integration```

