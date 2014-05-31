// Hermann.c

#include "hermann_lib.h"

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
	else if (!quiet)
		fprintf(stderr, "%% Message delivered (%zd bytes)\n", len);
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
	if (rkmessage->err) {
		if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
			fprintf(stderr,
				"%% Consumer reached end of %s [%"PRId32"] "
			       "message queue at offset %"PRId64"\n",
			       rd_kafka_topic_name(rkmessage->rkt),
			       rkmessage->partition, rkmessage->offset);

			if (exit_eof)
				run = 0;

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

	if (!quiet)
		fprintf(stdout, "%% Message (offset %"PRId64", %zd bytes):\n",
			rkmessage->offset, rkmessage->len);

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
	    fprintf(stderr, "Yield a value to block\n");
	    VALUE value = rb_str_new((char *)rkmessage->payload, rkmessage->len);
	    fprintf(stderr, "About to call yield\n");
	    rb_yield(value);
	} else {
	    fprintf(stderr, "No block given\n");
	}
}

static void sig_usr1 (int sig) {
	rd_kafka_dump(stdout, rk);
}

static void stop (int sig) {
	run = 0;
	fclose(stdin); /* abort fgets() */
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

// Main entry point for Consumer behavior
void actAsConsumer(char* topic) {

    fprintf(stderr, "actAsConsumer for topic %s\n", topic);

    /* Kafka configuration */
    rd_kafka_topic_t *rkt;
    char *brokers = "localhost:9092";
    char mode = 'C';
    int partition = 0; // todo: handle proper partitioning
    int opt;
    rd_kafka_conf_t *conf;
   	rd_kafka_topic_conf_t *topic_conf;
   	char errstr[512];
   	const char *debug = NULL;
   	int64_t start_offset = 0;
   	int do_conf_dump = 0;

   	quiet = !isatty(STDIN_FILENO);

    /* Kafka configuration */
	conf = rd_kafka_conf_new();
	fprintf(stderr, "Kafka configuration created\n");

	/* Topic configuration */
	topic_conf = rd_kafka_topic_conf_new();
	fprintf(stderr, "Topic configuration created\n");

	/* TODO: offset calculation */
	start_offset = RD_KAFKA_OFFSET_END;

    signal(SIGINT, stop);
    signal(SIGUSR1, sig_usr1);
    fprintf(stderr, "Signals sent\n");

    /* Consumer specific code */

    fprintf(stderr, "Create a Kafka handle\n");
    /* Create Kafka handle */
    if (!(rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr)))) {
        fprintf(stderr, "%% Failed to create new consumer: %s\n", errstr);
    	exit(1);
    }

    /* Set logger */
    /*rd_kafka_set_logger(rk, logger);
    fprintf(stderr, "Logger set\n");
    rd_kafka_set_log_level(rk, LOG_DEBUG);
    fprintf(stderr, "Loglevel configured\n");*/

    /* Add brokers */
    fprintf(stderr, "About to add brokers..");
    if (rd_kafka_brokers_add(rk, brokers) == 0) {
    fprintf(stderr, "%% No valid brokers specified\n");
    	exit(1);
    }
    fprintf(stderr, "Brokers added\n");

    /* Create topic */
    rkt = rd_kafka_topic_new(rk, topic, topic_conf);
    fprintf(stderr, "Topic created\n");

    /* Start consuming */
    if (rd_kafka_consume_start(rkt, partition, start_offset) == -1){
        fprintf(stderr, "%% Failed to start consuming: %s\n",
            rd_kafka_err2str(rd_kafka_errno2err(errno)));
        exit(1);
    }

    fprintf(stderr, "Consume started\n");

    /* Run loop */
    while (run) {
        rd_kafka_message_t *rkmessage;

        /* Consume single message.
         * See rdkafka_performance.c for high speed
         * consuming of messages. */
        rkmessage = rd_kafka_consume(rkt, partition, 1000);
        if (!rkmessage) /* timeout */
            continue;

        msg_consume(rkmessage, NULL);

        /* Return message to rdkafka */
        rd_kafka_message_destroy(rkmessage);
    }

    fprintf(stderr, "Run loop exited\n");

    /* Stop consuming */
    rd_kafka_consume_stop(rkt, partition);

    rd_kafka_topic_destroy(rkt);

    rd_kafka_destroy(rk);

	/* Let background threads clean up and terminate cleanly. */
	rd_kafka_wait_destroyed(2000);
}

// Ruby gem extensions
static VALUE consume(VALUE c, VALUE topicValue) {
    fprintf(stderr, "Called consume with one argument\n");

    char* topic = StringValueCStr(topicValue);

    fprintf(stderr, "Topic is: %s\n", topic);

    actAsConsumer(topic);

    return Qnil;
}

void Init_hermann_lib() {

    m_hermann = rb_define_module("Hermann");

    VALUE c_consumer = rb_define_class_under(m_hermann, "Consumer", rb_cObject);

    rb_define_method( c_consumer, "consume", consume, 1 );
}