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

/**
 * Mocks
 *
 */

#include "rdkafka_int.h"
#include "rdbuf.h"
#include "rdkafka_mock.h"


static void rd_kafka_mock_cluster_io_del (rd_kafka_mock_cluster_t *mcluster,
                                          int fd) {
        int i;

        for (i = 0 ; i < mcluster->fd_cnt ; i++) {
                if (mcluster->fds[i].fd == fd) {
                        if (i + 1 < mcluster->fd_cnt) {
                                memmove(&mcluster->fds[i],
                                        &mcluster->fds[i+1],
                                        sizeof(*mcluster->fds) *
                                        mcluster->fd_cnt - i);
                                memmove(&mcluster->handlers[i],
                                        &mcluster->handlers[i+1],
                                        sizeof(*mcluster->handlers) *
                                        mcluster->fd_cnt - i);
                        }

                        mcluster->fd_cnt--;
                        return;
                }
        }

        rd_assert(!*"mock_cluster_io_del: fd not found");
}


static void rd_kafka_mock_cluster_io_add (rd_kafka_mock_cluster_t *mcluster,
                                          int fd, int events,
                                          rd_kafka_mock_io_handler_t handler,
                                          void *opaque) {
        if (mcluster->fd_cnt + 1 > mcluster->fd_size) {
                mcluster->fd_size += 8;

                mcluster->fds = rd_realloc(mcluster->fds,
                                           sizeof(*mcluster->fds) *
                                           mcluster->fd_size);
                mcluster->handlers = rd_realloc(mcluster->handlers,
                                                sizeof(*mcluster->handlers) *
                                                mcluster->fd_size);
        }

        memset(&mcluster->fds[mcluster->fd_cnt], 0,
               sizeof(mcluster->fds[mcluster->fd_cnt]));
        mcluster->fds[mcluster->fd_cnt].fd = fd;
        mcluster->fds[mcluster->fd_cnt].events = events;
        mcluster->fds[mcluster->fd_cnt].revents = 0;
        mcluster->handlers[mcluster->fd_cnt].cb = handler;
        mcluster->handlers[mcluster->fd_cnt].opaque = opaque;
        mcluster->fd_cnt++;
}


static void rd_kafka_mock_connection_close (rd_kafka_mock_connection_t *mconn,
                                            const char *reason) {

        rd_kafka_dbg(mconn->broker->cluster->rk, MOCK, "MOCK",
                     "Mock connection from %s: %s",
                     rd_sockaddr2str(&mconn->peer, RD_SOCKADDR2STR_F_PORT),
                     reason);

        rd_kafka_mock_cluster_io_del(mconn->broker->cluster, mconn->s);
        TAILQ_REMOVE(&mconn->broker->connections, mconn, link);
        rd_close(mconn->s);
        rd_free(mconn);
}


/**
 * @returns 1 if a complete request is available in which case \p slicep
 *          is set to a new slice containing the data,
 *          0 if a complete request is not yet available,
 *          -1 on error.
 */
static int
rd_kafka_mock_connection_read_request (rd_kafka_mock_connection_t *mconn,
                                       rd_slice_t **slicep) {
        ssize_t r;
        int cnt = 0;
        void *p;
        size_t size;

        if (rd_buf_write_pos(&mconn->rxbuf) == 0) {
                /* Initial read for a protocol request.
                 * Allocate enough room for the protocol header
                 * (where the total size is located). */
                mconn->rxbuf = rd_kafka_buf_new(2, RD_KAFKAP_REQHDR_SIZE);

                rd_buf_write_ensure(&mconn->rxbuf,
                                    RD_KAFKAP_REQHDR_SIZE,
                                    RD_KAFKAP_REQHDR_SIZE);
        }

        /* Read as much data as possible from the socket into the
         * connection receive buffer. */
        while ((size = rd_buf_get_writable(&mconn->rxbuf, &p))) {
                ssize_t r;

                r = recv(mconn->s, p, size, MSG_DONTWAIT/*FIXME*/);
                if (r == 1) {
                        rd_kafka_dbg(mconn->broker->cluster->rk, MOCK, "MOCK",
                                     "Receive failed: %s", rd_strerror(errno));
                        return -1;
                } else if (r == 0) {
                        /* A zero read after POLLIN means the connection
                         * was closed. */
                        if (cnt == 0)
                                return -1;

                        break;
                }

                rd_buf_write(&mconn->rxbuf, NULL, r);

                if (rd_buf_write_pos(&mconn->rxbuf) == RD_KAFKAP_REQHDR_SIZE) {
                        /* Received the full header, now check full request
                         * size and allocate the buffer accordingly. */
                        struct rd_kafkap_reqhdr reqhdr;
                        rd_slice_t slice;

                        rd_slice_init(&slice, &mconn->rxbuf, 0,
                                      RD_KAFKAP_REQHDR_SIZE);
                        rd_slice_read_i32(&slice, &reqhdr.Size);
                        rd_slice_read_i16(&slice, &reqhdr.ApiKey);
                        rd_slice_read_i16(&slice, &reqhdr.ApiVersion);
                        rd_slice_read_i32(&slice, &reqhdr.CorrId);

                }
        }

        return 0;
}

/**
 * @brief Parse protocol request.
 *
 * @returns 0 on success, -1 on parse error.
 */
static int
rd_kafka_mock_connection_parse_request (rd_kafka_mock_connection_t *mconn,
                                        rd_slice_t *slice) {
        return 0;
}



/**
 * @brief Send as many bytes as possible from the output buffer.
 *
 * @returns the number of bytes written to socket, or -1 on error.
 */
static int
rd_kafka_mock_connection_write_out (rd_kafka_mock_connection_t *mconn) {


        return 0;
}



static void rd_kafka_mock_connection_write (rd_kafka_mock_connection_t *mconn,
                                            rd_buf_t *wbuf) {
        /* FIXME */
}

/**
 * @brief Per-Connection IO handler
 */
static void rd_kafka_mock_connection_io (rd_kafka_mock_cluster_t *mcluster,
                                         int fd, int events, void *opaque) {
        rd_kafka_mock_connection_t *mconn = opaque;

        if (events & POLLIN) {
                rd_slice_t *slice = NULL;
                int r;

                while ((r = rd_kafka_mock_connection_read_request(
                                mconn, &slice)) == 1) {

                        if (rd_kafka_mock_connection_parse_request(
                                    mconn, slice) == -1) {
                                rd_kafka_mock_connection_close(mconn,
                                                               "Parse error");
                                return;
                        }

                }

                if (r == -1) {
                        rd_kafka_mock_connection_close(mconn, "Read error");
                        return;
                }
        }

        if (events & (POLLERR|POLLHUP)) {
                rd_kafka_mock_connection_close(mconn, "Disconnected");
                return;
        }

        if (events & POLLOUT) {
                if (rd_kafka_mock_connection_write_out(mconn) == -1) {
                        rd_kafka_mock_connection_close(mconn, "Write error");
                        return;
                }
        }
}



static rd_kafka_mock_connection_t *
rd_kafka_mock_connection_new (rd_kafka_mock_broker_t *mrkb, int s,
                              const struct sockaddr_in *peer) {
        rd_kafka_mock_connection_t *mconn;

        mconn = rd_calloc(1, sizeof(*mconn));
        mconn->broker = mrkb;
        mconn->s = s;
        mconn->peer = *peer;

        TAILQ_INSERT_TAIL(&mrkb->connections, mconn, link);

        rd_kafka_mock_cluster_io_add(mrkb->cluster, mconn->s, POLLIN,
                                     rd_kafka_mock_connection_io, mconn);

        rd_kafka_dbg(mrkb->cluster->rk, MOCK, "MOCK",
                     "New connection to mock broker %"PRId32" from %s",
                     mrkb->broker_id,
                     rd_sockaddr2str(&mconn->peer, RD_SOCKADDR2STR_F_PORT));

        return mconn;
}



static void rd_kafka_mock_cluster_ctrl_io (rd_kafka_mock_cluster_t *mcluster,
                                           int fd, int events, void *opaque) {
        rd_kafka_dbg(mcluster->rk, MOCK, "MOCK", "Shutting down mock cluster");
        mcluster->run = rd_false;
}

static int rd_kafka_mock_cluster_thread_main (void *arg) {
        rd_kafka_mock_cluster_t *mcluster = arg;

        /* Control socket IO */
        rd_kafka_mock_cluster_io_add(mcluster, mcluster->ctrl_s[1],
                                     POLLIN|POLLHUP,
                                     rd_kafka_mock_cluster_ctrl_io, NULL);

        mcluster->run = rd_true;

        while (mcluster->run) {
                int r;
                int i;

#ifndef _MSC_VER
                r = poll(mcluster->fds, mcluster->fd_cnt, -1);
#else
                r = WSAPoll(mcluster->fds, mcluster->fd_cnt, -1);
#endif

                if (r == -1) {
                        rd_kafka_log(mcluster->rk, LOG_CRIT, "MOCK",
                                     "Mock cluster failed to poll %d fds: %s",
                                     mcluster->fd_cnt, rd_strerror(errno));
                        break;
                }

                for (i = 0 ; r > 0 && i < mcluster->fd_cnt ; i++) {
                        if (!mcluster->fds[i].revents)
                                continue;

                        /* Call IO handler */
                        mcluster->handlers[i].cb(mcluster, mcluster->fds[i].fd,
                                                 mcluster->fds[i].revents,
                                                 mcluster->handlers[i].opaque);
                        r--;
                }
        }

        rd_kafka_mock_cluster_io_del(mcluster, mcluster->ctrl_s[1]);

        return 0;
}





static void rd_kafka_mock_broker_listen_io (rd_kafka_mock_cluster_t *mcluster,
                                            int fd, int events, void *opaque) {
        rd_kafka_mock_broker_t *mrkb = opaque;

        if (events & (POLLERR|POLLHUP))
                rd_assert(!*"Mock broker listen socket error");

        if (events & POLLIN) {
                int new_s;
                struct sockaddr_in peer;
                socklen_t peer_size = sizeof(peer);

                new_s = accept(mrkb->listen_s, (struct sockaddr *)&peer,
                               &peer_size);
                if (new_s == -1) {
                        rd_kafka_log(mcluster->rk, LOG_ERR, "MOCK",
                                     "Failed to accept mock broker socket: %s",
                                     rd_strerror(errno));
                        return;
                }

                rd_kafka_mock_connection_new(mrkb, new_s, &peer);
        }
}


static void rd_kafka_mock_broker_destroy (rd_kafka_mock_broker_t *mrkb) {
        rd_kafka_mock_connection_t *mconn;

        while ((mconn = TAILQ_FIRST(&mrkb->connections)))
                rd_kafka_mock_connection_close(mconn, "Destroying broker");

        rd_kafka_mock_cluster_io_del(mrkb->cluster, mrkb->listen_s);
        rd_close(mrkb->listen_s);

        rd_free(mrkb);
}


static rd_kafka_mock_broker_t *
rd_kafka_mock_broker_new (rd_kafka_mock_cluster_t *mcluster,
                          int32_t broker_id) {
        rd_kafka_mock_broker_t *mrkb;
        int listen_s;
        struct sockaddr_in sin = {
                .sin_family = AF_INET,
                .sin_addr = {
                        .s_addr = htonl(INADDR_LOOPBACK)
                }
        };
        socklen_t sin_len = sizeof(sin);

        /*
         * Create and bind socket to any loopback port
         */
        listen_s = rd_kafka_socket_cb_linux(AF_INET, SOCK_STREAM, IPPROTO_TCP,
                                            NULL);
        if (listen_s == -1) {
                rd_kafka_log(mcluster->rk, LOG_CRIT, "MOCK",
                             "Unable to create mock broker listen socket: %s",
                             rd_strerror(errno));
                return NULL;
        }

        if (bind(listen_s, (struct sockaddr *)&sin, sizeof(sin)) == -1) {
                rd_kafka_log(mcluster->rk, LOG_CRIT, "MOCK",
                             "Failed to bind mock broker socket to %s: %s",
                             rd_strerror(errno),
                             rd_sockaddr2str(&sin, RD_SOCKADDR2STR_F_PORT));
                rd_close(listen_s);
                return NULL;
        }

        if (getsockname(listen_s, (struct sockaddr *)&sin, &sin_len) == -1) {
                rd_kafka_log(mcluster->rk, LOG_CRIT, "MOCK",
                             "Failed to get mock broker socket name: %s",
                             rd_strerror(errno));
                rd_close(listen_s);
                return NULL;
        }
        rd_assert(sin.sin_family == AF_INET);

        if (listen(listen_s, 5) == -1) {
                rd_kafka_log(mcluster->rk, LOG_CRIT, "MOCK",
                             "Failed to listen on mock broker socket: %s",
                             rd_strerror(errno));
                rd_close(listen_s);
                return NULL;
        }


        /*
         * Create mock broker object
         */
        mrkb = rd_calloc(1, sizeof(*mrkb));

        mrkb->cluster = mcluster;
        mrkb->listen_s = listen_s;

        rd_snprintf(mrkb->advertised_listener,
                    sizeof(mrkb->advertised_listener),
                    "%s", rd_sockaddr2str(&sin, RD_SOCKADDR2STR_F_PORT));

        TAILQ_INIT(&mrkb->connections);

        TAILQ_INSERT_TAIL(&mcluster->brokers, mrkb, link);

        rd_kafka_mock_cluster_io_add(mcluster, listen_s, POLLIN,
                                     rd_kafka_mock_broker_listen_io, mrkb);

        return mrkb;
}



static void
rd_kafka_mock_cluster_destroy0 (rd_kafka_mock_cluster_t *mcluster) {
        rd_kafka_mock_broker_t *mrkb;

        while ((mrkb = TAILQ_FIRST(&mcluster->brokers)))
                rd_kafka_mock_broker_destroy(mrkb);

        if (mcluster->fd_size > 0) {
                rd_free(mcluster->fds);
                rd_free(mcluster->handlers);
        }

        rd_free(mcluster);
}


/**
 * @brief Destroy cluster.
 *
 * @locality any thread
 */
void rd_kafka_mock_cluster_destroy (rd_kafka_mock_cluster_t *mcluster) {
        int res;

        rd_kafka_dbg(mcluster->rk, MOCK, "MOCK", "Destroying cluster");

        if (rd_write(mcluster->ctrl_s[0], "term", 4) == -1)
                rd_kafka_log(mcluster->rk, LOG_ERR, "MOCK",
                             "mock_cluster_destroy rd_write(%d) failure: %s",
                             mcluster->ctrl_s[0], rd_strerror(errno));

        thrd_join(mcluster->thread, &res);

        rd_close(mcluster->ctrl_s[0]);
        rd_close(mcluster->ctrl_s[1]);

        rd_kafka_mock_cluster_destroy0(mcluster);
}


rd_kafka_mock_cluster_t *rd_kafka_mock_cluster_new (rd_kafka_t *rk,
                                                    int broker_cnt) {
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_mock_broker_t *mrkb;
        int i;
        size_t bootstraps_len = 0;
        size_t of;

        mcluster = rd_calloc(1, sizeof(*mcluster));
        mcluster->rk = rk;
        TAILQ_INIT(&mcluster->brokers);

        for (i = 1 ; i <= broker_cnt ; i++) {
                if (!(mrkb = rd_kafka_mock_broker_new(mcluster, i))) {
                        rd_kafka_mock_cluster_destroy(mcluster);
                        return NULL;
                }

                bootstraps_len += strlen(mrkb->advertised_listener) + 1;
        }

        if (rd_pipe(mcluster->ctrl_s) == -1) {
                rd_kafka_log(rk, LOG_CRIT, "MOCK",
                             "Failed to create mock pipe: %s",
                             rd_strerror(errno));
                rd_kafka_mock_cluster_destroy(mcluster);
        }

        if (thrd_create(&mcluster->thread,
                        rd_kafka_mock_cluster_thread_main, mcluster) !=
            thrd_success) {
                rd_kafka_log(rk, LOG_CRIT, "MOCK",
                             "Failed to create mock cluster thread: %s",
                             rd_strerror(errno));
                rd_kafka_mock_cluster_destroy(mcluster);
                return NULL;
        }


        /* Construct bootstrap.servers list */
        mcluster->bootstraps = rd_malloc(bootstraps_len + 1);
        of = 0;
        TAILQ_FOREACH(mrkb, &mcluster->brokers, link) {
                size_t len = strlen(mrkb->advertised_listener);

                if (of > 0)
                        mcluster->bootstraps[of++] = ',';

                memcpy(&mcluster->bootstraps[of], mrkb->advertised_listener,
                       len);
                of += len;
        }
        mcluster->bootstraps[of] = '\0';

        rd_kafka_dbg(rk, MOCK, "MOCK", "Mock bootstrap.servers=%s",
                     mcluster->bootstraps);

        return mcluster;
}


const char *
rd_kafka_mock_cluster_bootstraps (const rd_kafka_mock_cluster_t *mcluster) {
        return mcluster->bootstraps;
}
