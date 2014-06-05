// Hermann.c

/* Much of the librdkafka library calls were lifted from rdkafka_example.c */

#include "hermann_lib.h"

/**
 * Utility functions
 */

void log_debug(char* msg) {
    if(DEBUG) {
        fprintf(stderr, "%s\n", msg);
    }
}

/**
 * Message delivery report callback.
 * Called once for each message.
 * See rdkafka.h for more information.
 */
static void msg_delivered (rd_kafka_t *rk,
			   void *payload, size_t len,
			   int error_code,
			   void *opaque, void *msg_opaque) {

	if (error_code)
		fprintf(stderr, "%% Message delivery failed: %s\n",
			rd_kafka_err2str(error_code));
}

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

static void msg_consume (rd_kafka_message_t *rkmessage,
			 void *opaque) {

    HermannInstanceConfig* cfg;
    uuid_string_t uuidStr;

    cfg = (HermannInstanceConfig*)opaque;

    if(DEBUG) {
        uuid_unparse(cfg->uuid, uuidStr);
        fprintf(stderr, "Consumer key: %s\n", uuidStr);
    }

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

	if (rkmessage->key_len) {
		if (output == OUTPUT_HEXDUMP)
			hexdump(stdout, "Message Key",
				rkmessage->key, rkmessage->key_len);
		else
			printf("Key: %.*s\n",
			       (int)rkmessage->key_len, (char *)rkmessage->key);
	}

	if (output == OUTPUT_HEXDUMP)
		hexdump(stdout, "Message Payload",
			rkmessage->payload, rkmessage->len);
	else
		printf("%.*s\n",
		       (int)rkmessage->len, (char *)rkmessage->payload);

    // Yield the data to the Consumer's block
	if(rb_block_given_p()) {
	    VALUE value = rb_str_new((char *)rkmessage->payload, rkmessage->len);
	    rb_yield(value);
	} else {
	    fprintf(stderr, "No block given\n"); // todo: should this be an error?
	}
}

/**
 * Kafka logger callback (optional)
 */
static void logger (const rd_kafka_t *rk, int level,
		    const char *fac, const char *buf) {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	fprintf(stderr, "%u.%03u RDKAFKA-%i-%s: %s: %s\n",
		(int)tv.tv_sec, (int)(tv.tv_usec / 1000),
		level, fac, rd_kafka_name(rk), buf);
}

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

/* Hermann::Consumer.consume */
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

    /* Run loop */
    while (consumerConfig->run) {
        rd_kafka_message_t *rkmessage;

        if(rd_kafka_consume_callback(consumerConfig->rkt, consumerConfig->partition,
        							      1000/*timeout*/,
        							      msg_consume,
        							      consumerConfig) < 0) {
            fprintf(stderr, "%% Error: %s\n", rd_kafka_err2str( rd_kafka_errno2err(errno)));
        }

    }

    /* Stop consuming */
    rd_kafka_consume_stop(consumerConfig->rkt, consumerConfig->partition);

    return Qnil;
}

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

    /* We're now initialized */
    config->isInitialized = 1;
}

/* Hermann::Producer.push(msg) */
static VALUE producer_push(VALUE self, VALUE message) {

    HermannInstanceConfig* producerConfig;
    char buf[2048];

    Data_Get_Struct(self, HermannInstanceConfig, producerConfig);

    if(producerConfig->topic==NULL) {
        fprintf(stderr, "Topic is null!");
        return;
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
		rd_kafka_poll(producerConfig->rk, 0);
	}

    /* Wait for messages to be delivered */
    while (producerConfig->run && rd_kafka_outq_len(producerConfig->rk) > 0)
        rd_kafka_poll(producerConfig->rk, 100);

}

/** Hermann::Producer.close */
static VALUE producer_close(VALUE self) {

   HermannInstanceConfig* producerConfig;

   Data_Get_Struct(self, HermannInstanceConfig, producerConfig);

   /* Destroy the handle */
   rd_kafka_destroy(producerConfig->rk);

   /* Let background threads clean up and terminate cleanly. */
   rd_kafka_wait_destroyed(2000);
}

/* Hermann::Producer.batch { block } */
static VALUE producer_batch(VALUE c) {
    /* todo: not implemented */
}

static void consumer_free(void * p) {
    // the p *should* contain a pointer to the consumerConfig which also must be freed
    // rd_kafka_topic_destroy(consumerConfig->rkt);

    // rd_kafka_destroy(consumerConfig->rk);

    /* todo: may or may not be necessary depending on how underlying threads are handled */
    /* Let background threads clean up and terminate cleanly. */
    // rd_kafka_wait_destroyed(2000);
}

static VALUE consumer_allocate(VALUE klass) {

    VALUE obj;

    HermannInstanceConfig* consumerConfig = ALLOC(HermannInstanceConfig);
    obj = Data_Wrap_Struct(klass, 0, consumer_free, consumerConfig);

    log_debug("Consumer allocated");

    return obj;
}

static VALUE consumer_initialize(VALUE self, VALUE topic) {

    HermannInstanceConfig* consumerConfig;
    char* topicPtr;

    topicPtr = StringValuePtr(topic);
    Data_Get_Struct(self, HermannInstanceConfig, consumerConfig);

    uuid_generate(consumerConfig->uuid);
    consumerConfig->topic = topicPtr;
    consumerConfig->brokers = "localhost:9092";
    consumerConfig->partition = 0;
    consumerConfig->run = 1;
    consumerConfig->exit_eof = 0;
    consumerConfig->quiet = 0;

    log_debug("Consumer initialized");

    return self;
}

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

static void producer_free(void * p) {
    /* todo: not implemented */
    /* Destroy the handle */
    // Handle is in the config, the config is probably in that p pointer
    // rd_kafka_destroy(producerConfig->rk);
}

static VALUE producer_allocate(VALUE klass) {

    VALUE obj;

    HermannInstanceConfig* producerConfig = ALLOC(HermannInstanceConfig);
    obj = Data_Wrap_Struct(klass, 0, producer_free, producerConfig);

    log_debug("Producer allocated");

    return obj;
}

static VALUE producer_initialize(VALUE self, VALUE topic) {

    HermannInstanceConfig* producerConfig;
    char* topicPtr;

    topicPtr = StringValuePtr(topic);
    Data_Get_Struct(self, HermannInstanceConfig, producerConfig);

    uuid_generate(producerConfig->uuid);
    producerConfig->topic = topicPtr;
    producerConfig->brokers = "localhost:9092";
    producerConfig->partition = 0;
    producerConfig->run = 1;
    producerConfig->exit_eof = 0;
    producerConfig->quiet = 0;

    log_debug("Producer initialized");

    return self;
}

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

void Init_hermann_lib() {

    /* Define the module */
    m_hermann = rb_define_module("Hermann");

    /* ---- Define the consumer class ---- */
    VALUE c_consumer = rb_define_class_under(m_hermann, "Consumer", rb_cObject);

    /* Allocate */
    rb_define_alloc_func(c_consumer, consumer_allocate);

    /* Initialize */
    rb_define_method(c_consumer, "initialize", consumer_initialize, 1);
    rb_define_method(c_consumer, "initialize_copy", consumer_init_copy, 1);

    /* Consumer has method 'consume' */
    rb_define_method( c_consumer, "consume", consumer_consume, 0 );

    /* ---- Define the producer class ---- */
    VALUE c_producer = rb_define_class_under(m_hermann, "Producer", rb_cObject);

    /* Allocate */
    rb_define_alloc_func(c_producer, producer_allocate);

    /* Initialize */
    rb_define_method(c_producer, "initialize", producer_initialize, 1);
    rb_define_method(c_producer, "initialize_copy", producer_init_copy, 1);

    /* Producer.push(msg) */
    rb_define_method( c_producer, "push", producer_push, 1 );

    /* Producer.batch(array) */
    rb_define_method( c_producer, "batch", producer_batch, 1 );

    /* Producer.close() */
    rb_define_method( c_producer, "close", producer_close, 0 );
}