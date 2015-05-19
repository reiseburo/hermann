# Hacking on Hermann


### Integration Testing

* Download Kafka
* Start Zookeeper
 * set port 2181
* Start Kafka
  * Set properties file ```zookeeper.connect=localhost:2181```

You can also use a docker instance like this one : https://github.com/spotify/docker-kafka
On mac : 
* ```boot2docker start ```
* ```$(boot2docker shellinit)```
* ```docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=`boot2docker ip` --env ADVERTISED_PORT=9092 spotify/kafka```
* ```export ZOOKEEPER=`boot2docker ip`:2181```
* ```export KAFKA=`boot2docker ip`:9092  ```
* modify ./fixtures/integration.yml with values in $KAKFA and $ZOOKEEPER

#### With JRuby

* ```bundle exec jruby -S rspec spec/integration```

#### With MRI

* ```bundle install```
* ```rake default``` or ```rake compile``` and then ```rake spec```
* ```rake spec:intregration```

