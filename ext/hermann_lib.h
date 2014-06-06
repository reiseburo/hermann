#ifndef HERMANN_H
#define HERMANN_H

#include <ruby.h>

#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <syslog.h>
#include <sys/time.h>
#include <errno.h>

#include <librdkafka/rdkafka.h>

// Holds the defined Ruby module for Hermann
static VALUE m_hermann;

static int DEBUG = 0;

static 	enum {
	OUTPUT_HEXDUMP,
	OUTPUT_RAW,
} output = OUTPUT_HEXDUMP;

typedef struct HermannInstanceConfig {

    char* topic;

    /* Kafka configuration */
    rd_kafka_t *rk;
    rd_kafka_topic_t *rkt;
    char *brokers;
    int partition;
    rd_kafka_topic_conf_t *topic_conf;
    char errstr[512];
    rd_kafka_conf_t *conf;
    const char *debug;
    int64_t start_offset;
    int do_conf_dump;

    int run;
    int exit_eof;
    int quiet;

    int isInitialized;

} HermannInstanceConfig;

#endif