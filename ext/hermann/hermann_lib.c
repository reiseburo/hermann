/*
 * hermann_lib.c - Ruby wrapper for the librdkafka library
 *
 * Copyright (c) 2014 Stan Campbell
 * Copyright (c) 2014 Lookout, Inc.
 * Copyright (c) 2014 R. Tyler Croy
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *	this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *	this list of conditions and the following disclaimer in the documentation
 *	and/or other materials provided with the distribution.
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
 * Convenience function
 *
 * @param   config  HermannInstanceConfig
 * @param   outputStream	FILE*
 *
 * Log the contents of the configuration to the provided stream.
 */
void fprintf_hermann_instance_config(HermannInstanceConfig *config,
									 FILE *outputStream) {

	const char *topic = NULL;
	const char *brokers = NULL;
	int isRkSet = -1;
	int isRktSet = -1;
	int partition = -1;
	int isInitialized = -1;

	if (NULL == config) {
		fprintf(outputStream, "NULL configuration");
	}
	else {
		isRkSet = (config->rk != NULL);
		isRktSet = (config->rkt != NULL);

		if (NULL == config->topic) {
			topic = NULL;
		}
		else {
			topic = config->topic;
		}

		if (NULL == config->brokers) {
			brokers = "NULL";
		}
		else {
			brokers = config->brokers;
		}

		partition = config->partition;
		isInitialized = config->isInitialized;
	}

	fprintf(outputStream, "{ topic: %s, brokers: %s, partition: %d, isInitialized: %d, rkSet: %d, rkTSet: %d }\n",
		topic, brokers, partition, isInitialized, isRkSet, isRktSet );
}

/**
 * Message delivery report callback.
 * Called once for each message.
 *
 */
static void msg_delivered(rd_kafka_t *rk,
						  const rd_kafka_message_t *message,
						  void *ctx) {
	hermann_push_ctx_t *push_ctx;
	VALUE is_error = Qfalse;
	ID hermann_result_fulfill_method = rb_intern("internal_set_value");

	TRACER("ctx: %p, err: %i\n", ctx, message->err);

	if (message->err) {
		is_error = Qtrue;
		fprintf(stderr, "%% Message delivery failed: %s\n",
			rd_kafka_err2str(message->err));
		/* todo: should raise an error? */
	}

	/* according to @edenhill rd_kafka_message_t._private is ABI safe to call
	 * and represents the `msg_opaque` argument passed into `rd_kafka_produce`
	 */
	if (NULL != message->_private) {
		push_ctx = (hermann_push_ctx_t *)message->_private;

		if (!message->err) {
			/* if we have not errored, great! let's say we're connected */
			push_ctx->producer->isConnected = 1;
		}

		/* call back into our Hermann::Result if it exists, discarding the
		* return value
		*/
		if (NULL != push_ctx->result) {
			rb_funcall(push_ctx->result,
						hermann_result_fulfill_method,
						2,
						rb_str_new((char *)message->payload, message->len), /* value */
						is_error /* is_error */ );
		}
		free(push_ctx);
	}
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
static int32_t producer_partitioner_callback(const rd_kafka_topic_t *rkt,
											 const void *keydata,
											 size_t keylen,
											 int32_t partition_cnt,
											 void *rkt_opaque,
											 void *msg_opaque) {
	/* Pick a random partition */
	int retry = 0;
	for (; retry < partition_cnt; retry++) {
		int32_t partition = rand() % partition_cnt;
		if (rd_kafka_topic_partition_available(rkt, partition)) {
			break; /* this one will do */
		}
	}
}

/**
 * hexdump
 *
 * Write the given payload to file in hex notation.
 *
 * @param fp	FILE*   the file into which to write
 * @param name  char*   name
 * @param ptr   void*   payload
 * @param len   size_t  payload length
 */
static void hexdump(FILE *fp,
					const char *name,
					const void *ptr,
					size_t len) {
	const char *p = (const char *)ptr;
	unsigned int of = 0;


	if (name) {
		fprintf(fp, "%s hexdump (%zd bytes):\n", name, len);
	}

	for (of = 0 ; of < len ; of += 16) {
		char hexen[16*3+1];
		char charen[16+1];
		unsigned int hof = 0;

		unsigned int cof = 0;
		unsigned int i;

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
 * @param rkmessage	rd_kafka_message_t* the message
 * @param opaque	   void*   opaque context
 */
static void msg_consume(rd_kafka_message_t *rkmessage,
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

			if (cfg->exit_eof) {
				cfg->run = 0;
			}

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
		if (output == OUTPUT_HEXDUMP) {
			hexdump(stdout, "Message Key",
				rkmessage->key, rkmessage->key_len);
		}
		else {
			printf("Key: %.*s\n",
				   (int)rkmessage->key_len, (char *)rkmessage->key);
		}
	}

	if (output == OUTPUT_HEXDUMP) {
		if (DEBUG) {
			hexdump(stdout, "Message Payload", rkmessage->payload, rkmessage->len);
		}
	}
	else {
		if (DEBUG) {
			printf("%.*s\n", (int)rkmessage->len, (char *)rkmessage->payload);
		}
	}

	// Yield the data to the Consumer's block
	if (rb_block_given_p()) {
		VALUE value = rb_str_new((char *)rkmessage->payload, rkmessage->len);
		rb_yield(value);
	}
	else {
		if (DEBUG) {
			fprintf(stderr, "No block given\n"); // todo: should this be an error?
		}
	}
}

/**
 * logger
 *
 * Kafka logger callback (optional)
 *
 * todo:  introduce better logging
 *
 * @param rk	   rd_kafka_t  the producer or consumer
 * @param level	int		 the log level
 * @param fac	  char*	   something of which I am unaware
 * @param buf	  char*	   the log message
 */
static void logger(const rd_kafka_t *rk,
				   int level,
				   const char *fac,
				   const char *buf) {
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

	TRACER("configuring rd_kafka\n");

	config->quiet = !isatty(STDIN_FILENO);

	/* Kafka configuration */
	config->conf = rd_kafka_conf_new();

	/* Topic configuration */
	config->topic_conf = rd_kafka_topic_conf_new();

	/* Create Kafka handle */
	if (!(config->rk = rd_kafka_new(RD_KAFKA_CONSUMER, config->conf,
		config->errstr, sizeof(config->errstr)))) {
		fprintf(stderr, "%% Failed to create new consumer: %s\n", config->errstr);
		rb_raise(rb_eRuntimeError, "%% Failed to create new consumer: %s\n", config->errstr);
	}

	/* Set logger */
	rd_kafka_set_logger(config->rk, logger);
	rd_kafka_set_log_level(config->rk, LOG_DEBUG);

	/* TODO: offset calculation */
	config->start_offset = RD_KAFKA_OFFSET_END;

	/* Add brokers */
	if (rd_kafka_brokers_add(config->rk, config->brokers) == 0) {
		fprintf(stderr, "%% No valid brokers specified\n");
		rb_raise(rb_eRuntimeError, "No valid brokers specified");
		return;
	}

	/* Create topic */
	config->rkt = rd_kafka_topic_new(config->rk, config->topic, config->topic_conf);

	/* We're now initialized */
	config->isInitialized = 1;
}

// Ruby gem extensions

#ifdef RB_THREAD_BLOCKING_REGION
/* NOTE: We only need this method defined if RB_THREAD_BLOCKING_REGION is
 * defined, otherwise it's unused
 */

/**
 * Callback invoked if Ruby needs to stop our Consumer's IO loop for any reason
 * (system exit, etc.)
 */
static void consumer_consume_stop_callback(void *ptr) {
	HermannInstanceConfig* config = (HermannInstanceConfig*)ptr;

	TRACER("stopping callback (%p)\n", ptr);

	config->run = 0;
}
#endif

/**
 * Loop on a timeout to receive messages from Kafka.  When the consumer_consume_stop_callback is invoked by Ruby,
 * we'll break out of our loop and return.
 */
void consumer_consume_loop(HermannInstanceConfig* consumerConfig) {

	TRACER("\n");

	while (consumerConfig->run) {
		if (rd_kafka_consume_callback(consumerConfig->rkt, consumerConfig->partition,
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
 * @param   VALUE   self	the Ruby object for this consumer
 */
static VALUE consumer_consume(VALUE self) {

	HermannInstanceConfig* consumerConfig;

	TRACER("starting consume\n");

	Data_Get_Struct(self, HermannInstanceConfig, consumerConfig);

	if ((NULL == consumerConfig->topic) ||
		(0 == strlen(consumerConfig->topic))) {
		fprintf(stderr, "Topic is null!\n");
		rb_raise(rb_eRuntimeError, "Topic cannot be empty");
		return self;
	}

	if (!consumerConfig->isInitialized) {
		consumer_init_kafka(consumerConfig);
	}

	/* Start consuming */
	if (rd_kafka_consume_start(consumerConfig->rkt, consumerConfig->partition, consumerConfig->start_offset) == -1) {
		fprintf(stderr, "%% Failed to start consuming: %s\n",
			rd_kafka_err2str(rd_kafka_errno2err(errno)));
		rb_raise(rb_eRuntimeError,
				rd_kafka_err2str(rd_kafka_errno2err(errno)));
		return Qnil;
	}

#ifdef RB_THREAD_BLOCKING_REGION
	/** The consumer will listen for incoming messages in a loop, timing out and checking the consumerConfig->run
	 *  flag every second.
	 *
	 *  Call rb_thread_blocking_region to release the GVM lock and allow Ruby to amuse itself while we wait on
	 *  IO from Kafka.
	 *
	 *  If Ruby needs to interrupt the consumer loop, the stop callback will be invoked and the loop should exit.
	 */
	rb_thread_blocking_region(consumer_consume_loop,
							  consumerConfig,
							  consumer_consume_stop_callback,
							  consumerConfig);
#else
	consumer_consume_loop(consumerConfig);
#endif


	/* Stop consuming */
	rd_kafka_consume_stop(consumerConfig->rkt, consumerConfig->partition);

	return Qnil;
}


static void producer_error_callback(rd_kafka_t *rk,
									int error,
									const char *reason,
									void *opaque) {
	hermann_conf_t *conf = (hermann_conf_t *)rd_kafka_opaque(rk);

	TRACER("error (%i): %s\n", error, reason);

	conf->isErrored = error;

	if (error) {
		/* If we have an old error string in here we need to make sure to
		 * free() it before we allocate a new string
		 */
		if (NULL != conf->error) {
			free(conf->error);
		}

		/* Grab the length of the string plus the null character */
		size_t error_length = strnlen(reason, HERMANN_MAX_ERRSTR_LEN) + 1;
		conf->error = (char *)malloc((sizeof(char) * error_length));
		(void)strncpy(conf->error, reason, error_length);
	}
}


/**
 *  producer_init_kafka
 *
 *  Initialize the producer instance, setting up the Kafka topic and context.
 *
 *  @param  self    VALUE Instance of the Producer Ruby object
 *  @param  config  HermannInstanceConfig*  the instance configuration associated with this producer.
 */
void producer_init_kafka(VALUE self, HermannInstanceConfig* config) {

	TRACER("initing (%p)\n", config);

	config->quiet = !isatty(STDIN_FILENO);

	/* Kafka configuration */
	config->conf = rd_kafka_conf_new();


	/* Add our `self` to the opaque pointer for error and logging callbacks
	 */
	rd_kafka_conf_set_opaque(config->conf, (void*)config);
	rd_kafka_conf_set_error_cb(config->conf, producer_error_callback);

	/* Topic configuration */
	config->topic_conf = rd_kafka_topic_conf_new();

	/* Set up a message delivery report callback.
	 * It will be called once for each message, either on successful
	 * delivery to broker, or upon failure to deliver to broker. */
	rd_kafka_conf_set_dr_msg_cb(config->conf, msg_delivered);

	/* Create Kafka handle */
	if (!(config->rk = rd_kafka_new(RD_KAFKA_PRODUCER,
									config->conf,
									config->errstr,
									sizeof(config->errstr)))) {
		/* TODO: Use proper logger */
		fprintf(stderr,
		"%% Failed to create new producer: %s\n", config->errstr);
		rb_raise(rb_eRuntimeError, "%% Failed to create new producer: %s\n", config->errstr);
	}

	/* Set logger */
	rd_kafka_set_logger(config->rk, logger);
	rd_kafka_set_log_level(config->rk, LOG_DEBUG);

	if (rd_kafka_brokers_add(config->rk, config->brokers) == 0) {
		/* TODO: Use proper logger */
		fprintf(stderr, "%% No valid brokers specified\n");
		rb_raise(rb_eRuntimeError, "No valid brokers specified");
		return;
	}

	/* Create topic */
	config->rkt = rd_kafka_topic_new(config->rk, config->topic, config->topic_conf);

	/* Set the partitioner callback */
	rd_kafka_topic_conf_set_partitioner_cb( config->topic_conf, producer_partitioner_callback);

	/* We're now initialized */
	config->isInitialized = 1;

	TRACER("completed kafka init\n");
}

/**
 *  producer_push_single
 *
 *  @param  self	VALUE   the Ruby producer instance
 *  @param  message VALUE   the ruby String containing the outgoing message.
 *  @param  result  VALUE   the Hermann::Result object to be fulfilled when the
 *		push completes
 */
static VALUE producer_push_single(VALUE self, VALUE message, VALUE result) {

	HermannInstanceConfig* producerConfig;
	/* Context pointer, pointing to `result`, for the librdkafka delivery
	 * callback
	 */
	hermann_push_ctx_t *delivery_ctx = (hermann_push_ctx_t *)malloc(sizeof(hermann_push_ctx_t));

	TRACER("self: %p, message: %p, result: %p)\n", self, message, result);

	Data_Get_Struct(self, HermannInstanceConfig, producerConfig);

	delivery_ctx->producer = producerConfig;
	delivery_ctx->result = NULL;

	TRACER("producerConfig: %p\n", producerConfig);

	if ((NULL == producerConfig->topic) ||
		(0 == strlen(producerConfig->topic))) {
		fprintf(stderr, "Topic is null!\n");
		rb_raise(rb_eRuntimeError, "Topic cannot be empty");
		return self;
	}

   	if (!producerConfig->isInitialized) {
		producer_init_kafka(self, producerConfig);
	}

	TRACER("kafka initialized\n");

	/* Only pass result through if it's non-nil */
	if (Qnil != result) {
		delivery_ctx->result = result;
		TRACER("setting result: %p\n", result);
	}

	/* Send/Produce message. */
	if (-1 == rd_kafka_produce(producerConfig->rkt,
						 producerConfig->partition,
						 RD_KAFKA_MSG_F_COPY,
						 rb_string_value_cstr(&message),
						 RSTRING_LEN(message),
						 NULL,
						 0,
						 delivery_ctx)) {
		fprintf(stderr, "%% Failed to produce to topic %s partition %i: %s\n",
					rd_kafka_topic_name(producerConfig->rkt), producerConfig->partition,
					rd_kafka_err2str(rd_kafka_errno2err(errno)));
		/* TODO: raise a Ruby exception here, requires a test though */
	}

	TRACER("returning\n");

	return self;
}

/**
 * producer_tick
 *
 * This function is responsible for ticking the librdkafka reactor so we can
 * get feedback from the librdkafka threads back into the Ruby environment
 *
 *  @param  self	VALUE   the Ruby producer instance
 *  @param  message VALUE   A Ruby FixNum of how many ms we should wait on librdkafka
 */
static VALUE producer_tick(VALUE self, VALUE timeout) {
	hermann_conf_t *conf = NULL;
	long timeout_ms = 0;
	int events = 0;

	if (Qnil != timeout) {
		timeout_ms = rb_num2int(timeout);
	}
	else {
		rb_raise(rb_eArgError, "Cannot call `tick` with a nil timeout!\n");
	}

	Data_Get_Struct(self, hermann_conf_t, conf);

	/*
	 * if the producerConfig is not initialized then we never properly called
	 * producer_push_single, so why are we ticking?
	 */
	if (!conf->isInitialized) {
		rb_raise(rb_eRuntimeError, "Cannot call `tick` without having ever sent a message\n");
	}

	events = rd_kafka_poll(conf->rk, timeout_ms);

	if (conf->isErrored) {
		rb_raise(rb_eStandardError, conf->error);
	}

	return rb_int_new(events);
}


static VALUE producer_connect(VALUE self, VALUE timeout) {
	HermannInstanceConfig *producerConfig;
	rd_kafka_resp_err_t err;
	VALUE result = Qfalse;
	int timeout_ms = rb_num2int(timeout);
	struct rd_kafka_metadata *data = NULL;

	Data_Get_Struct(self, HermannInstanceConfig, producerConfig);

	if (!producerConfig->isInitialized) {
		producer_init_kafka(self, producerConfig);
	}

	err = rd_kafka_metadata(producerConfig->rk,
									0,
								   producerConfig->rkt,
								   &data,
								   timeout_ms);
	TRACER("err: %s (%i)\n", rd_kafka_err2str(err), err);
	
	if (RD_KAFKA_RESP_ERR_NO_ERROR == err) {
		TRACER("brokers: %i, topics: %i\n",
				data->broker_cnt,
				data->topic_cnt);
		producerConfig->isConnected = 1;
		result = Qtrue;
	}
	else {
		producerConfig->isErrored = err;
	}

	rd_kafka_metadata_destroy(data);

	return result;
}

static VALUE producer_is_connected(VALUE self) {
	HermannInstanceConfig *producerConfig;

	Data_Get_Struct(self, HermannInstanceConfig, producerConfig);

	if (!producerConfig->isInitialized) {
		return Qfalse;
	}

	if (!producerConfig->isConnected) {
		return Qfalse;
	}

	return Qtrue;
}

static VALUE producer_is_errored(VALUE self) {
	HermannInstanceConfig *producerConfig;

	Data_Get_Struct(self, HermannInstanceConfig, producerConfig);

	if (producerConfig->isErrored) {
		return Qtrue;
	}

	return Qfalse;
}


/**
 *  consumer_free
 *
 *  Callback called when Ruby needs to GC the configuration associated with an Hermann instance.
 *
 *  @param  p   void*   the instance of an HermannInstanceConfig to be freed from allocated memory.
 */
static void consumer_free(void *p) {

	HermannInstanceConfig* config = (HermannInstanceConfig *)p;

#ifdef TRACE
	fprintf(stderr, "consumer_free\n");
#endif

	// the p *should* contain a pointer to the consumerConfig which also must be freed
	if (config->rkt != NULL) {
		rd_kafka_topic_destroy(config->rkt);
	}

	if (config->rk != NULL) {
		rd_kafka_destroy(config->rk);
	}

	// clean up the struct
	free(config);
}

/**
 *  consumer_allocate
 *
 *  Allocate and wrap an HermannInstanceConfig for this Consumer object.
 *
 *  @param klass	VALUE   the class of the enclosing Ruby object.
 */
static VALUE consumer_allocate(VALUE klass) {

	VALUE obj;
	HermannInstanceConfig* consumerConfig;

#ifdef TRACE
	fprintf(stderr, "consumer_free\n");
#endif

	consumerConfig = ALLOC(HermannInstanceConfig);

	// Make sure it's initialized
	consumerConfig->topic = NULL;
	consumerConfig->rk = NULL;
	consumerConfig->rkt = NULL;
	consumerConfig->brokers = NULL;
	consumerConfig->partition = -1;
	consumerConfig->topic_conf = NULL;
	consumerConfig->errstr[0] = 0;
	consumerConfig->conf = NULL;
	consumerConfig->debug = NULL;
	consumerConfig->start_offset = -1;
	consumerConfig->do_conf_dump = -1;
	consumerConfig->run = 0;
	consumerConfig->exit_eof = 0;
	consumerConfig->quiet = 0;
	consumerConfig->isInitialized = 0;

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
 *  @param  self		VALUE   the Ruby instance of the Consumer
 *  @param  topic	   VALUE   a Ruby string
 *  @param  brokers	 VALUE   a Ruby string containing list of host:port
 *  @param  partition   VALUE   a Ruby number
 */
static VALUE consumer_initialize(VALUE self,
								 VALUE topic,
								 VALUE brokers,
								 VALUE partition) {

	HermannInstanceConfig* consumerConfig;
	char* topicPtr;
	char* brokersPtr;
	int partitionNo;

	TRACER("initing consumer ruby object\n");

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
 *  @param  copy	VALUE   the Ruby Consumer instance (with configuration) as destination
 *  @param  orig	VALUE   the Ruby Consumer instance (with configuration) as source
 *
 */
static VALUE consumer_init_copy(VALUE copy,
								VALUE orig) {
	HermannInstanceConfig* orig_config;
	HermannInstanceConfig* copy_config;

	if (copy == orig) {
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
static void producer_free(void *p) {

	HermannInstanceConfig* config = (HermannInstanceConfig *)p;

	TRACER("dealloc producer ruby object (%p)\n", p);


	if (NULL == p) {
		return;
	}

	// Clean up the topic
	if (NULL != config->rkt) {
		rd_kafka_topic_destroy(config->rkt);
	}

	// Take care of the producer instance
	if (NULL != config->rk) {
		rd_kafka_destroy(config->rk);
	}

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

	producerConfig->topic = NULL;
	producerConfig->rk = NULL;
	producerConfig->rkt = NULL;
	producerConfig->brokers = NULL;
	producerConfig->partition = -1;
	producerConfig->topic_conf = NULL;
	producerConfig->errstr[0] = 0;
	producerConfig->conf = NULL;
	producerConfig->debug = NULL;
	producerConfig->start_offset = -1;
	producerConfig->do_conf_dump = -1;
	producerConfig->run = 0;
	producerConfig->exit_eof = 0;
	producerConfig->quiet = 0;
	producerConfig->isInitialized = 0;
	producerConfig->isConnected = 0;
	producerConfig->isErrored = 0;
	producerConfig->error = NULL;

	obj = Data_Wrap_Struct(klass, 0, producer_free, producerConfig);

	return obj;
}

/**
 *  producer_initialize
 *
 *  Set up the configuration context for the Producer instance
 *
 *  @param  self	VALUE   the Producer instance
 *  @param  topic   VALUE   the Ruby string naming the topic
 *  @param  brokers VALUE   a Ruby string containing host:port pairs separated by commas
 */
static VALUE producer_initialize(VALUE self,
								 VALUE topic,
								 VALUE brokers) {

	HermannInstanceConfig* producerConfig;
	char* topicPtr;
	char* brokersPtr;

	TRACER("initialize Producer ruby object\n");


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
 *  @param  copy	VALUE   destination Producer
 *  @param  orign   VALUE   source Producer
 */
static VALUE producer_init_copy(VALUE copy,
								VALUE orig) {
	HermannInstanceConfig* orig_config;
	HermannInstanceConfig* copy_config;

	if (copy == orig) {
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
	VALUE lib_module, c_consumer, c_producer;

	TRACER("setting up Hermann::Lib\n");

	/* Define the module */
	hermann_module = rb_define_module("Hermann");
	lib_module = rb_define_module_under(hermann_module, "Lib");


	/* ---- Define the consumer class ---- */
	c_consumer = rb_define_class_under(lib_module, "Consumer", rb_cObject);

	/* Allocate */
	rb_define_alloc_func(c_consumer, consumer_allocate);

	/* Initialize */
	rb_define_method(c_consumer, "initialize", consumer_initialize, 3);
	rb_define_method(c_consumer, "initialize_copy", consumer_init_copy, 1);

	/* Consumer has method 'consume' */
	rb_define_method( c_consumer, "consume", consumer_consume, 0 );

	/* ---- Define the producer class ---- */
	c_producer = rb_define_class_under(lib_module, "Producer", rb_cObject);

	/* Allocate */
	rb_define_alloc_func(c_producer, producer_allocate);

	/* Initialize */
	rb_define_method(c_producer, "initialize", producer_initialize, 2);
	rb_define_method(c_producer, "initialize_copy", producer_init_copy, 1);

	/* Producer.push_single(msg) */
	rb_define_method(c_producer, "push_single", producer_push_single, 2);

	/* Producer.tick */
	rb_define_method(c_producer, "tick", producer_tick, 1);

	/* Producer.connected? */
	rb_define_method(c_producer, "connected?", producer_is_connected, 0);

	/* Producer.errored? */
	rb_define_method(c_producer, "errored?", producer_is_errored, 0);

	/* Producer.connect */
	rb_define_method(c_producer, "connect", producer_connect, 1);
}
