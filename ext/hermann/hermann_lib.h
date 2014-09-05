/*
 * hermann_lib.h - Ruby wrapper for the librdkafka library
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

#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <syslog.h>
#include <sys/time.h>
#include <errno.h>

#include <librdkafka/rdkafka.h>

#undef TRACE

// Holds the defined Ruby module for Hermann
static VALUE hermann_module;

static int DEBUG = 0;

// Hold the system signal handler
static void (*ruby_vm_sighandler)(int) = NULL;

// Global klaxon announcing we're going down
// 0 - normal operation
// 1 - we're going down
static int KLAXON = 0;

// Should we expect rb_thread_blocking_region to be present?
// #define RB_THREAD_BLOCKING_REGION
#undef RB_THREAD_BLOCKING_REGION

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

	/* Optional pointer to executable block */
	VALUE *block;

	int run;
	int exit_eof;
	int quiet;

	int isInitialized;

} HermannInstanceConfig;

#endif
