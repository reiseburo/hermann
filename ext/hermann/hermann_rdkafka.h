/*
 * hermann_rdkafka.h - Ruby wrapper for the librdkafka library
 *
 * Copyright (c) 2014 Stan Campbell
 * Copyright (c) 2014 Lookout, Inc.
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

#ifndef HERMANN_H
#define HERMANN_H

#include <ruby.h>

#ifdef HAVE_RUBY_THREAD_H
#include <ruby/thread.h>
#endif

#ifdef HAVE_RUBY_INTERN_H
#include <ruby/intern.h>
#endif

#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <syslog.h>
#include <sys/time.h>
#include <errno.h>

#include <librdkafka/rdkafka.h>

#ifdef TRACE
#define TRACER(...) do {  \
	fprintf(stderr, "%i:%s()> ", __LINE__, __PRETTY_FUNCTION__); \
	fprintf(stderr, __VA_ARGS__); \
	fflush(stderr); \
					} while (0)
#else
#define TRACER(...) do { } while (0)
#endif

// Holds the defined Ruby module for Hermann
static VALUE hermann_module;

#define HERMANN_MAX_ERRSTR_LEN 512

static int DEBUG = 0;

// Should we expect rb_thread_blocking_region to be present?
// #define RB_THREAD_BLOCKING_REGION
#undef RB_THREAD_BLOCKING_REGION

static 	enum {
	OUTPUT_HEXDUMP,
	OUTPUT_RAW,
} output = OUTPUT_HEXDUMP;

typedef struct HermannInstanceConfig {
	char *topic;

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
	int isConnected;

	int isErrored;

	char *error;
} HermannInstanceConfig;

typedef HermannInstanceConfig hermann_conf_t;

typedef struct {
	/* Hermann::Provider::RDKafka::Producer */
	hermann_conf_t *producer;
	/* Hermann::Result */
	VALUE result;
} hermann_push_ctx_t;

typedef struct {
	rd_kafka_t *rk;
	rd_kafka_topic_t *topic;
	struct rd_kafka_metadata *data;
	int timeout_ms;
} hermann_metadata_ctx_t;

#endif
