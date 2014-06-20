/*
 * hermann_lib.c - Ruby wrapper for the librdkafka library
 *
 * Copyright (c) 2014 Stan Campbell
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/* Much of the librdkafka library calls were lifted from rdkafka_example.c */

#include "hermann_lib.h"

/**
 * Utility functions
 */


/**
 * Convenience function
 *
 * @param msg   char*       the string to be logged under debugging.
 */
void log_debug(char* msg) {
    if(DEBUG) {
        fprintf(stderr, "%s\n", msg);
    }
}

/**
 * Message delivery report callback.
 * Called once for each message.
 *
 * @param rk    rd_kafka_t* instance of producer or consumer
 * @param payload   void*   the payload of the message
 * @param len   size_t  the length of the payload in bytes
 * @param error_code    int
 * @param opaque    void*   optional context
 * @param msg_opaque    void*   it's opaque
 */
static void msg_delivered (rd_kafka_t *rk,
			   void *payload, size_t len,
			   int error_code,
			   void *opaque, void *msg_opaque) {

	if (error_code)
		fprintf(stderr, "%% Message delivery failed: %s\n",
			rd_kafka_err2str(error_code));
}

/**
 * Producer partitioner callback.
 * Used to determine the target partition within a topic for production.
 *
 * Returns an integer partition number or RD_KAFKA_PARTITION_UA if no
 * available partition could be determined.
 *
 * @param   rkt rd_kafka_topic_t*   the topic
 * @param   keydata void*   key information for calculating the partition
 * @param   keylen  size_t  key size
 * @param   partition_cnt   int32_t the count of the number of partitions
 * @param   rkt_opaque  void*   opaque topic info
 * @param   msg_opaque  void*   opaque message info
 */
static int32_t producer_paritioner_callback( const rd_kafka_topic_t *rkt,
                                      const void *keydata,
                                      size_t keylen,
                                      int32_t partition_cnt,
                                      void *rkt_opaque,
                                      void *msg_opaque) {
    /* Pick a random partition */
    int retry;
    for(retry=0;retry<partition_cnt;retry++) {
        int32_t partition = rand() % partition_cnt;
        if(rd_kafka_topic_partition_available(rkt, partition)) {
            break; /* this one will do */
        }
    }
}

/**
 * hexdump
 *
 * Write the given payload to file in hex notation.
 *
 * @param fp    FILE*   the file into which to write
 * @param name  char*   name
 * @param ptr   void*   payload
 * @param len   size_t  payload length
 */
static void hexdump (FILE *fp, const char *name, const void *ptr, size_t len) {
	const char *p = (const char *)ptr;
	int of = 0;


	if (name)
		fprintf(fp, "%s hexdump (%zd bytes):\n", name, len);

	for (of = 0 ; of < len ; of += 16) {
		char hexen[16*3+1];
		char charen[16+1];
		int hof = 0;

		int cof = 0;
		int i;

		for (i = of ; i < of + 16 && i < len ; i++) {
			hof += sprintf(hexen+hof, "%02x ", p[i] & 0xff);
			cof += sprintf(charen+cof, "%c",
				       isprint((int)p[i]) ? p[i] : '.');
		}
		fprintf(fp, "%08x: %-48s %-16s\n",
			of, hexen, charen);
	}
}

/**
 * msg_consume
 *
 * Callback on message receipt.
 *
 * @param rkmessage    rd_kafka_message_t* the message
 * @param opaque       void*   opaque context
 */
static void msg_consume (rd_kafka_message_t *rkmessage,
			 void *opaque) {

    HermannInstanceConfig* cfg;

    cfg = (HermannInstanceConfig*)opaque;

	if (rkmessage->err) {
		if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
			fprintf(stderr,
				"%% Consumer reached end of %s [%"PRId32"] "
			       "message queue at offset %"PRId64"\n",
			       rd_kafka_topic_name(rkmessage->rkt),
			       rkmessage->partition, rkmessage->offset);

			if (cfg->exit_eof)
				cfg->run = 0;

			return;
		}

		fprintf(stderr, "%% Consume error for topic \"%s\" [%"PRId32"] "
		       "offset %"PRId64": %s\n",
		       rd_kafka_topic_name(rkmessage->rkt),
		       rkmessage->partition,
		       rkmessage->offset,
		       rd_kafka_message_errstr(rkmessage));
		return;
	}

	if (DEBUG && rkmessage->key_len) {
		if (output == OUTPUT_HEXDUMP)
			hexdump(stdout, "Message Key",
				rkmessage->key, rkmessage->key_len);
		else
			printf("Key: %.*s\n",
			       (int)rkmessage->key_len, (char *)rkmessage->key);
	}

	if (output == OUTPUT_HEXDUMP) {
		if(DEBUG)
			hexdump(stdout, "Message Payload", rkmessage->payload, rkmessage->len);
	} else {
		if(DEBUG)
			printf("%.*s\n", (int)rkmessage->len, (char *)rkmessage->payload);
	}

    // Yield the data to the Consumer's block
	if(rb_block_given_p()) {
	    VALUE value = rb_str_new((char *)rkmessage->payload, rkmessage->len);
	    rb_yield(value);
	} else {
	    if(DEBUG)
			fprintf(stderr, "No block given\n"); // todo: should this be an error?
	}
}

/**
 * logger
 *
 * Kafka logger callback (optional)
 *
 * todo:  introduce better logging
 *
 * @param rk       rd_kafka_t  the producer or consumer
 * @param level    int         the log level
 * @param fac      char*       something of which I am unaware
 * @param buf      char*       the log message
 */
static void logger (const rd_kafka_t *rk, int level,
		    const char *fac, const char *buf) {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	fprintf(stderr, "%u.%03u RDKAFKA-%i-%s: %s: %s\n",
		(int)tv.tv_sec, (int)(tv.tv_usec / 1000),
		level, fac, rd_kafka_name(rk), buf);
}

/**
 * consumer_init_kafka
 *
 * Initialize the Kafka context and instantiate a consumer.
 *
 * @param   config  HermannInstanceConfig*  pointer to the instance configuration for this producer or consumer
 */
void consumer_init_kafka(HermannInstanceConfig* config) {

    config->quiet = !isatty(STDIN_FILENO);

    /* Kafka configuration */
    config->conf = rd_kafka_conf_new();

    /* Topic configuration */
    config->topic_conf = rd_kafka_topic_conf_new();

    /* Create Kafka handle */
    if (!(config->rk = rd_kafka_new(RD_KAFKA_CONSUMER, config->conf,
        config->errstr, sizeof(config->errstr)))) {
        fprintf(stderr, "%% Failed to create new consumer: %s\n", config->errstr);
        exit(1);
    }
	
	/* Set logger */
    rd_kafka_set_logger(config->rk, logger);
    rd_kafka_set_log_level(config->rk, LOG_DEBUG);

    /* TODO: offset calculation */
    config->start_offset = RD_KAFKA_OFFSET_END;

    /* Add brokers */
    if(rd_kafka_brokers_add(config->rk, config->brokers) == 0) {
        fprintf(stderr, "%% No valid brokers specified\n");
        exit(1);
    }

    /* Create topic */
    config->rkt = rd_kafka_topic_new(config->rk, config->topic, config->topic_conf);

    /* We're now initialized */
    config->isInitialized = 1;
}

// Ruby gem extensions

/**
 * Callback invoked if Ruby needs to stop our Consumer's IO loop for any reason (system exit, etc.)
 */
static void consumer_consume_stop_callback(void *ptr) {
    HermannInstanceConfig* config = (HermannInstanceConfig*)ptr;

    config->run = 0;
}

/**
 * Loop on a timeout to receive messages from Kafka.  When the consumer_consume_stop_callback is invoked by Ruby,
 * we'll break out of our loop and return.
 */
void consumer_consume_loop(HermannInstanceConfig* consumerConfig) {

    while (consumerConfig->run) {
        rd_kafka_message_t *rkmessage;

        if(rd_kafka_consume_callback(consumerConfig->rkt, consumerConfig->partition,
        							      1000/*timeout*/,
        							      msg_consume,
        							      consumerConfig) < 0) {
            fprintf(stderr, "%% Error: %s\n", rd_kafka_err2str( rd_kafka_errno2err(errno)));
        }

    }
}

/**
 * Hermann::Consumer.consume
 *
 * Begin listening on the configured topic for messages.  msg_consume will be called on each message received.
 *
 * @param   VALUE   self    the Ruby object for this consumer
 */
static VALUE consumer_consume(VALUE self) {

    HermannInstanceConfig* consumerConfig;

    Data_Get_Struct(self, HermannInstanceConfig, consumerConfig);

    if(consumerConfig->topic==NULL) {
            fprintf(stderr, "Topic is null!");
            return;
    }

    if(!consumerConfig->isInitialized) {
        consumer_init_kafka(consumerConfig);
    }

    /* Start consuming */
    if (rd_kafka_consume_start(consumerConfig->rkt, consumerConfig->partition, consumerConfig->start_offset) == -1){
        fprintf(stderr, "%% Failed to start consuming: %s\n",
            rd_kafka_err2str(rd_kafka_errno2err(errno)));
        exit(1);
    }

    /** The consumer will listen for incoming messages in a loop, timing out and checking the consumerConfig->run
     *  flag every second.
     *
     *  Call rb_thread_blocking_region to release the GVM lock and allow Ruby to amuse itself while we wait on
     *  IO from Kafka.
     *
     *  If Ruby needs to interrupt the consumer loop, the stop callback will be invoked and the loop should exit.
     */
    rb_thread_blocking_region(consumer_consume_loop, consumerConfig, consumer_consume_stop_callback, consumerConfig);


    /* Stop consuming */
    rd_kafka_consume_stop(consumerConfig->rkt, consumerConfig->partition);

    return Qnil;
}

/**
 *  producer_init_kafka
 *
 *  Initialize the producer instance, setting up the Kafka topic and context.
 *
 *  @param  config  HermannInstanceConfig*  the instance configuration associated with this producer.
 */
void producer_init_kafka(HermannInstanceConfig* config) {

    config->quiet = !isatty(STDIN_FILENO);

    /* Kafka configuration */
    config->conf = rd_kafka_conf_new();

    /* Topic configuration */
    config->topic_conf = rd_kafka_topic_conf_new();

    /* Set up a message delivery report callback.
     * It will be called once for each message, either on successful
     * delivery to broker, or upon failure to deliver to broker. */
    rd_kafka_conf_set_dr_cb(config->conf, msg_delivered);

    /* Create Kafka handle */
    if (!(config->rk = rd_kafka_new(RD_KAFKA_PRODUCER, config->conf, config->errstr, sizeof(config->errstr)))) {
        fprintf(stderr,
        "%% Failed to create new producer: %s\n", config->errstr);
        exit(1);
    }

    /* Set logger */
    rd_kafka_set_logger(config->rk, logger);
    rd_kafka_set_log_level(config->rk, LOG_DEBUG);

    if(rd_kafka_brokers_add(config->rk, config->brokers) == 0) {
        fprintf(stderr, "%% No valid brokers specified\n");
        exit(1);
    }

    /* Create topic */
    config->rkt = rd_kafka_topic_new(config->rk, config->topic, config->topic_conf);

    /* Set the partitioner callback */
    rd_kafka_topic_conf_set_partitioner_cb( config->topic_conf, producer_paritioner_callback );

    /* We're now initialized */
    config->isInitialized = 1;
}

/**
 *  producer_push_single
 *
 *  @param  self    VALUE   the Ruby producer instance
 *  @param  message VALUE   the ruby String containing the outgoing message.
 */
static VALUE producer_push_single(VALUE self, VALUE message) {

    HermannInstanceConfig* producerConfig;
    char buf[2048];

    Data_Get_Struct(self, HermannInstanceConfig, producerConfig);

    if(producerConfig->topic==NULL) {
        fprintf(stderr, "Topic is null!");
        return self;
    }

   	if(!producerConfig->isInitialized) {
   	    producer_init_kafka(producerConfig);
    }

    char *msg = StringValueCStr(message);
    strcpy(buf, msg);

    size_t len = strlen(buf);
    if (buf[len-1] == '\n')
        buf[--len] = '\0';

    /* Send/Produce message. */
	if (rd_kafka_produce(producerConfig->rkt, producerConfig->partition, RD_KAFKA_MSG_F_COPY,
        /* Payload and length */
		buf, len,
		/* Optional key and its length */
		NULL, 0,
		/* Message opaque, provided in
		* delivery report callback as
		* msg_opaque. */
		NULL) == -1) {

	    fprintf(stderr, "%% Failed to produce to topic %s partition %i: %s\n",
					rd_kafka_topic_name(producerConfig->rkt), producerConfig->partition,
					rd_kafka_err2str(rd_kafka_errno2err(errno)));

        /* Poll to handle delivery reports */
		rd_kafka_poll(producerConfig->rk, 10);
	}

    /* Must poll to handle delivery reports */
    rd_kafka_poll(producerConfig->rk, 0);

    return self;
}

/**
 *  producer_push_array
 *
 *  Publish each of the messages in array on the configured topic.
 *
 *  @param  self    VALUE   the instance of the Ruby Producer object
 *  @param  length  int     the length of the outgoing messages array
 *  @param  array   VALUE   the Ruby array of messages
 */
static VALUE producer_push_array(VALUE self, int length, VALUE array) {

    int i;
    VALUE message;

    for(i=0;i<length;i++) {
        message = RARRAY_PTR(array)[i];
        producer_push_single(self, message);
    }

    return self;
}

/**
 *  Hermann::Producer.push(msg)
 *
 *  Publish the given message on the configured topic.
 *
 *  @param  self    VALUE   the Ruby instance of the Producer.
 *  @param  message VALUE   the Ruby string containing the message.
 */
static VALUE producer_push(VALUE self, VALUE message) {

    VALUE arrayP = rb_check_array_type(message);

    if(!NIL_P(arrayP)) {
        return producer_push_array(self, RARRAY_LEN(arrayP), message);
    } else {
        return producer_push_single(self, message);
    }
}

/**
 *  consumer_free
 *
 *  Callback called when Ruby needs to GC the configuration associated with an Hermann instance.
 *
 *  @param  p   void*   the instance of an HermannInstanceConfig to be freed from allocated memory.
 */
static void consumer_free(void * p) {

    HermannInstanceConfig* config = (HermannInstanceConfig *)p;

    // the p *should* contain a pointer to the consumerConfig which also must be freed
    rd_kafka_topic_destroy(config->rkt);

    rd_kafka_destroy(config->rk);

    // clean up the struct
    free(config);
}

/**
 *  consumer_allocate
 *
 *  Allocate and wrap an HermannInstanceConfig for this Consumer object.
 *
 *  @param klass    VALUE   the class of the enclosing Ruby object.
 */
static VALUE consumer_allocate(VALUE klass) {

    VALUE obj;

    HermannInstanceConfig* consumerConfig = ALLOC(HermannInstanceConfig);
    obj = Data_Wrap_Struct(klass, 0, consumer_free, consumerConfig);

    return obj;
}

/**
 *  consumer_initialize
 *
 *  todo: configure the brokers through passed parameter, later through zk
 *
 *  Set up the Consumer's HermannInstanceConfig context.
 *
 *  @param  self        VALUE   the Ruby instance of the Consumer
 *  @param  topic       VALUE   a Ruby string
 *  @param  brokers     VALUE   a Ruby string containing list of host:port
 *  @param  partition   VALUE   a Ruby number
 */
static VALUE consumer_initialize(VALUE self, VALUE topic, VALUE brokers, VALUE partition) {

    HermannInstanceConfig* consumerConfig;
    char* topicPtr;
    char* brokersPtr;
    int partitionNo;

    topicPtr = StringValuePtr(topic);
    brokersPtr = StringValuePtr(brokers);
    partitionNo = FIX2INT(partition);
    Data_Get_Struct(self, HermannInstanceConfig, consumerConfig);

    consumerConfig->topic = topicPtr;
    consumerConfig->brokers = brokersPtr;
    consumerConfig->partition = partitionNo;
    consumerConfig->run = 1;
    consumerConfig->exit_eof = 0;
    consumerConfig->quiet = 0;

    return self;
}

/**
 *  consumer_init_copy
 *
 *  When copying into a new instance of a Consumer, reproduce the configuration info.
 *
 *  @param  copy    VALUE   the Ruby Consumer instance (with configuration) as destination
 *  @param  orig    VALUE   the Ruby Consumer instance (with configuration) as source
 *
 */
static VALUE consumer_init_copy(VALUE copy, VALUE orig) {
    HermannInstanceConfig* orig_config;
    HermannInstanceConfig* copy_config;

    if(copy == orig) {
        return copy;
    }

    if (TYPE(orig) != T_DATA || RDATA(orig)->dfree != (RUBY_DATA_FUNC)consumer_free) {
        rb_raise(rb_eTypeError, "wrong argument type");
    }

    Data_Get_Struct(orig, HermannInstanceConfig, orig_config);
    Data_Get_Struct(copy, HermannInstanceConfig, copy_config);

    // Copy over the data from one struct to the other
    MEMCPY(copy_config, orig_config, HermannInstanceConfig, 1);

    return copy;
}

/**
 *  producer_free
 *
 *  Reclaim memory allocated to the Producer's configuration
 *
 *  @param  p   void*   the instance's configuration struct
 */
static void producer_free(void * p) {

    HermannInstanceConfig* config = (HermannInstanceConfig *)p;

    // Clean up the topic
    rd_kafka_topic_destroy(config->rkt);

    // Take care of the producer instance
    rd_kafka_destroy(config->rk);

    // Free the struct
    free(config);
}

/**
 *  producer_allocate
 *
 *  Allocate the memory for a Producer's configuration
 *
 *  @param  klass   VALUE   the class of the Producer
 */
static VALUE producer_allocate(VALUE klass) {

    VALUE obj;

    HermannInstanceConfig* producerConfig = ALLOC(HermannInstanceConfig);
    obj = Data_Wrap_Struct(klass, 0, producer_free, producerConfig);

    return obj;
}

/**
 *  producer_initialize
 *
 *  Set up the configuration context for the Producer instance
 *
 *  @param  self    VALUE   the Producer instance
 *  @param  topic   VALUE   the Ruby string naming the topic
 *  @param  brokers VALUE   a Ruby string containing host:port pairs separated by commas
 */
static VALUE producer_initialize(VALUE self, VALUE topic, VALUE brokers) {

    HermannInstanceConfig* producerConfig;
    char* topicPtr;
    char* brokersPtr;

    topicPtr = StringValuePtr(topic);
    brokersPtr = StringValuePtr(brokers);
    Data_Get_Struct(self, HermannInstanceConfig, producerConfig);
	
    producerConfig->topic = topicPtr;
    producerConfig->brokers = brokersPtr;
    /** Using RD_KAFKA_PARTITION_UA specifies we want the partitioner callback to be called to determine the target
     *  partition
     */
    producerConfig->partition = RD_KAFKA_PARTITION_UA;
    producerConfig->run = 1;
    producerConfig->exit_eof = 0;
    producerConfig->quiet = 0;

    return self;
}

/**
 *  producer_init_copy
 *
 *  Copy the configuration information from orig into copy for the given Producer instances.
 *
 *  @param  copy    VALUE   destination Producer
 *  @param  orign   VALUE   source Producer
 */
static VALUE producer_init_copy(VALUE copy, VALUE orig) {
    HermannInstanceConfig* orig_config;
    HermannInstanceConfig* copy_config;

    if(copy == orig) {
        return copy;
    }

    if (TYPE(orig) != T_DATA || RDATA(orig)->dfree != (RUBY_DATA_FUNC)producer_free) {
        rb_raise(rb_eTypeError, "wrong argument type");
    }

    Data_Get_Struct(orig, HermannInstanceConfig, orig_config);
    Data_Get_Struct(copy, HermannInstanceConfig, copy_config);

    // Copy over the data from one struct to the other
    MEMCPY(copy_config, orig_config, HermannInstanceConfig, 1);

    return copy;
}

/**
 * Init_hermann_lib
 *
 * Called by Ruby when the Hermann gem is loaded.
 * Defines the Hermann module.
 * Defines the Producer and Consumer classes.
 */
void Init_hermann_lib() {

    /* Define the module */
    m_hermann = rb_define_module("Hermann");

    /* ---- Define the consumer class ---- */
    VALUE c_consumer = rb_define_class_under(m_hermann, "Consumer", rb_cObject);

    /* Allocate */
    rb_define_alloc_func(c_consumer, consumer_allocate);

    /* Initialize */
    rb_define_method(c_consumer, "initialize", consumer_initialize, 3);
    rb_define_method(c_consumer, "initialize_copy", consumer_init_copy, 1);

    /* Consumer has method 'consume' */
    rb_define_method( c_consumer, "consume", consumer_consume, 0 );

    /* ---- Define the producer class ---- */
    VALUE c_producer = rb_define_class_under(m_hermann, "Producer", rb_cObject);

    /* Allocate */
    rb_define_alloc_func(c_producer, producer_allocate);

    /* Initialize */
    rb_define_method(c_producer, "initialize", producer_initialize, 2);
    rb_define_method(c_producer, "initialize_copy", producer_init_copy, 1);

    /* Producer.push(msg) */
    rb_define_method( c_producer, "push", producer_push, 1 );

}