/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2019 Magnus Edenhill
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

#ifndef _RDKAFKA_MOCK_H_
#define _RDKAFKA_MOCK_H_


/**
 * @struct A real TCP connection from the client to a mock broker.
 */
typedef struct rd_kafka_mock_connection_s {
        TAILQ_ENTRY(rd_kafka_mock_connection_s) link;
        int    s;                /**< Socket */
        rd_buf_t *rxbuf;         /**< Receive buffer */
        rd_buf_t *txbuf;         /**< Send buffer */
        struct sockaddr_in peer; /**< Peer address */
        struct rd_kafka_mock_broker_s *broker;
} rd_kafka_mock_connection_t;


/**
 * @struct Mock broker
 */
typedef struct rd_kafka_mock_broker_s {
        TAILQ_ENTRY(rd_kafka_mock_broker_s) link;
        char    advertised_listener[128];
        int32_t broker_id;

        int     listen_s;   /**< listen() socket */

        TAILQ_HEAD(, rd_kafka_mock_connection_s) connections;

        struct rd_kafka_mock_cluster_s *cluster;
} rd_kafka_mock_broker_t;


typedef void (rd_kafka_mock_io_handler_t) (struct rd_kafka_mock_cluster_s
                                           *mcluster,
                                           int fd, int events, void *opaque);

/**
 * @struct Mock cluster.
 *
 * The cluster IO loop runs in a separate thread where all
 * broker IO is handled.
 *
 * No locking is needed.
 */
typedef struct rd_kafka_mock_cluster_s {
        rd_kafka_t *rk;
        TAILQ_HEAD(, rd_kafka_mock_broker_s) brokers;

        char *bootstraps; /**< bootstrap.servers */

        thrd_t thread;    /**< Mock thread */
        int ctrl_s[2];    /**< Control socket for terminating the mock thread.
                           *   [1] is the thread's read socket */

        rd_bool_t run;    /**< Cluster will run while this value is true */

        int                         fd_cnt;   /**< Number of file descriptors */
        int                         fd_size;  /**< Allocated size of .fds
                                               *   and .handlers */
        struct pollfd              *fds;      /**< Dynamic array */

        /**< Dynamic array of IO handlers for corresponding fd in .fds */
        struct {
                rd_kafka_mock_io_handler_t *cb; /**< Callback */
                void *opaque;                   /**< Callbacks' opaque */
        } *handlers;
} rd_kafka_mock_cluster_t;



void rd_kafka_mock_cluster_destroy (rd_kafka_mock_cluster_t *mcluster);
rd_kafka_mock_cluster_t *rd_kafka_mock_cluster_new (rd_kafka_t *rk,
                                                    int broker_cnt);
const char *
rd_kafka_mock_cluster_bootstraps (const rd_kafka_mock_cluster_t *mcluster);

#endif /* _RDKAFKA_MOCK_H_ */
