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
#include "rdkafka_interceptor.h"
#include "rdkafka_mock.h"
#include "rdkafka_transport_int.h"



static int
rd_kafka_mock_partition_assign_replicas (rd_kafka_mock_partition_t *mpart) {
        rd_kafka_mock_cluster_t *mcluster = mpart->topic->cluster;
        int replica_cnt = RD_MIN(mcluster->defaults.replication_factor,
                                 mcluster->broker_cnt);
        rd_kafka_mock_broker_t *mrkb;
        int i;

        if (mpart->replicas)
                rd_free(mpart->replicas);

        mpart->replicas = rd_calloc(replica_cnt, sizeof(*mpart->replicas));
        mpart->replica_cnt = replica_cnt;

        /* FIXME: randomize this using perhaps reservoir sampling */
        TAILQ_FOREACH(mrkb, &mcluster->brokers, link) {
                if (i == mpart->replica_cnt)
                        break;
                mpart->replicas[i++] = mrkb;
        }

        /* Select a random leader */
        mpart->leader = mpart->replicas[rd_jitter(0, replica_cnt)];
}


/**
 * @brief Destroy resources for partition, but the \p mpart itself is not freed.
 */
static void rd_kafka_mock_partition_destroy (rd_kafka_mock_partition *mpart) {
        rd_free(mpart->replicas);
}


static void rd_kafka_mock_partition_init (rd_kafka_mock_topic_t *mtopic,
                                          rd_kafka_mock_partition_t *mpart,
                                          int id, int replication_factor) {
        int i;

        mpart->topic = mtopic;
        mpart->id = id;

        rd_kafka_mock_partition_assign_replicas(mpart);
}


static void rd_kafka_mock_topic_destroy (rd_kafka_mock_topic_t *mtopic) {
        int i;

        for (i = 0 ; i < mtopic->partition_cnt ; i++)
                rd_kafka_mock_partition_destroy(&mtopic->partition[i]);

        TAILQ_REMOVE(&mtopic->cluster->topics, mtopic, link);
        rd_free(mtopic->partitions);
        rd_free(mtopic->name);
        rd_free(mtopic);
}


static rd_kafka_mock_topic_t *
rd_kafka_mock_topic_new (rd_kafka_mock_cluster_t *mcluster, const char *topic,
                         int partition_cnt, int replication_factor) {
        rd_kafka_mock_topic_t *mtopic;
        int i;

        mtopic = rd_calloc(1, sizeof(*mtopic));
        mtopic->name = rd_strdup(topic);
        mtopic->cluster = mcluster;

        mtopic->partitions = rd_calloc(partition_cnt,
                                       sizeof(*mtopic->partitions));

        for (i = 0 ; i < partition_cnt ; i++)
                rd_kafka_mock_partition_init(mtopic, &mtopic->partition[i],
                                             i, replication_factor);

        TAILQ_INSERT_TAIL(&mcluster->topics, mtopic, link);
        mcluster->topic_cnt++;

        return mtopic;
}


static rd_kafka_mock_topic_t *
rd_kafka_mock_topic_find (const rd_kafka_mock_cluster_t *mcluster,
                          const char *name) {
        const rd_kafka_mock_topic_t *mtopic;

        TAILQ_FOREACH(mtopic, &mcluster->topics, link) {
                if (!strcmp(mtopic->name, name))
                        return (rd_kafka_mock_topic_t *)mtopic;
        }

        return NULL;
}

static rd_kafka_mock_topic_t *
rd_kafka_mock_topic_auto_create (rd_kafka_mock_cluster_t *mcluster,
                                 const char *topic, rd_kafka_resp_err_t *errp) {
        rd_kafka_mock_topic_t *mtopic;

        

}

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


/**
 * @brief Add \p fd to IO poll with initial desired events (POLLIN, et.al).
 *
 * @returns a pointer to the events bitmask, which the caller may update
 *          at any time to add/remove poll flags.
 */
static short *rd_kafka_mock_cluster_io_add (rd_kafka_mock_cluster_t *mcluster,
                                            int fd, int events,
                                            rd_kafka_mock_io_handler_t handler,
                                            void *opaque) {
        short *poll_events;

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
        poll_events = &mcluster->fds[mcluster->fd_cnt].events;
        mcluster->fd_cnt++;

        return poll_events;
}


static void rd_kafka_mock_connection_close (rd_kafka_mock_connection_t *mconn,
                                            const char *reason) {

        rd_kafka_dbg(mconn->broker->cluster->rk, MOCK, "MOCK",
                     "Mock connection from %s: %s",
                     rd_sockaddr2str(&mconn->peer, RD_SOCKADDR2STR_F_PORT),
                     reason);

        if (mconn->rxbuf)
                rd_kafka_buf_destroy(mconn->rxbuf);

        rd_kafka_mock_cluster_io_del(mconn->broker->cluster,
                                     mconn->transport->rktrans_s);
        TAILQ_REMOVE(&mconn->broker->connections, mconn, link);
        rd_kafka_transport_close(mconn->transport);
        rd_free(mconn);
}


static void
rd_kafka_mock_connection_send_response (rd_kafka_mock_connection_t *mconn,
                                        const rd_kafka_buf_t *request,
                                        rd_kafka_buf_t *resp) {

        resp->rkbuf_reshdr.Size = rd_buf_write_pos(&resp->rkbuf_buf) - 4;

        rd_kafka_buf_update_i32(resp, 0, resp->rkbuf_reshdr.Size);

        rd_kafka_dbg(mconn->broker->cluster->rk, MOCK, "MOCK",
                     "Broker %"PRId32": Sending %sResponseV%hd to %s",
                     mconn->broker->id,
                     rd_kafka_ApiKey2str(request->rkbuf_reqhdr.ApiKey),
                     request->rkbuf_reqhdr.ApiVersion,
                     rd_sockaddr2str(&mconn->peer, RD_SOCKADDR2STR_F_PORT));

        /* Set up a buffer reader for sending the buffer. */
        rd_slice_init_full(&resp->rkbuf_reader, &resp->rkbuf_buf);

        rd_kafka_bufq_enq(&mconn->outbufs, resp);

        *mconn->poll_events |= POLLOUT;
}


/**
 * @returns 1 if a complete request is available in which case \p slicep
 *          is set to a new slice containing the data,
 *          0 if a complete request is not yet available,
 *          -1 on error.
 */
static int
rd_kafka_mock_connection_read_request (rd_kafka_mock_connection_t *mconn,
                                       rd_kafka_buf_t **rkbufp) {
        rd_kafka_t *rk = mconn->broker->cluster->rk;
        const rd_bool_t log_decode_errors = rd_true;
        rd_kafka_buf_t *rkbuf;
        char errstr[128];
        ssize_t r;

        if (!(rkbuf = mconn->rxbuf)) {
                /* Initial read for a protocol request.
                 * Allocate enough room for the protocol header
                 * (where the total size is located). */
                rkbuf = mconn->rxbuf = rd_kafka_buf_new(2,
                                                        RD_KAFKAP_REQHDR_SIZE);

                /* Protocol parsing code needs the rkb for logging */
                rkbuf->rkbuf_rkb = mconn->broker->cluster->dummy_rkb;
                rd_kafka_broker_keep(rkbuf->rkbuf_rkb);

                /* Make room for request header */
                rd_buf_write_ensure(&rkbuf->rkbuf_buf,
                                    RD_KAFKAP_REQHDR_SIZE,
                                    RD_KAFKAP_REQHDR_SIZE);
        }

        /* Read as much data as possible from the socket into the
         * connection receive buffer. */
        r = rd_kafka_transport_recv(mconn->transport, &rkbuf->rkbuf_buf,
                                    errstr, sizeof(errstr));
        if (r == -1) {
                rd_kafka_dbg(rk, MOCK, "MOCK",
                             "Mock broker %"PRId32": connection %s: "
                             "receive failed: %s",
                             mconn->broker->id,
                             rd_sockaddr2str(&mconn->peer,
                                             RD_SOCKADDR2STR_F_PORT),
                             errstr);
                return -1;
        } else if (r == 0) {
                return 0; /* Need more data */
        }

        if (rd_buf_write_pos(&rkbuf->rkbuf_buf) ==
            RD_KAFKAP_REQHDR_SIZE) {
                /* Received the full header, now check full request
                 * size and allocate the buffer accordingly. */

                /* Initialize reader */
                rd_slice_init(&rkbuf->rkbuf_reader,
                              &rkbuf->rkbuf_buf, 0,
                              RD_KAFKAP_REQHDR_SIZE);

                rd_kafka_buf_read_i32(rkbuf,
                                      &rkbuf->rkbuf_reqhdr.Size);
                rd_kafka_buf_read_i16(rkbuf,
                                      &rkbuf->rkbuf_reqhdr.ApiKey);
                rd_kafka_buf_read_i16(rkbuf,
                                      &rkbuf->rkbuf_reqhdr.ApiVersion);
                rd_kafka_buf_read_i32(rkbuf,
                                      &rkbuf->rkbuf_reqhdr.CorrId);

                rkbuf->rkbuf_totlen = rkbuf->rkbuf_reqhdr.Size + 4;

                if (rkbuf->rkbuf_totlen < RD_KAFKAP_REQHDR_SIZE + 2 ||
                    rkbuf->rkbuf_totlen >
                    (size_t)rk->rk_conf.recv_max_msg_size) {
                        rd_kafka_buf_parse_fail(
                                rkbuf,
                                "Invalid request size %"PRId32
                                " from %s",
                                rkbuf->rkbuf_reqhdr.Size,
                                rd_sockaddr2str(
                                        &mconn->peer,
                                        RD_SOCKADDR2STR_F_PORT));
                        RD_NOTREACHED();
                }

                /* Now adjust totlen to skip the header */
                rkbuf->rkbuf_totlen -= RD_KAFKAP_REQHDR_SIZE;

                if (!rkbuf->rkbuf_totlen) {
                        /* Empty request (valid) */
                        *rkbufp = rkbuf;
                        mconn->rxbuf = NULL;
                        return 1;
                }

                /* Allocate space for the request payload */
                rd_buf_write_ensure(&rkbuf->rkbuf_buf,
                                    rkbuf->rkbuf_totlen,
                                    rkbuf->rkbuf_totlen);

        } else if (rd_buf_write_pos(&rkbuf->rkbuf_buf) -
                   RD_KAFKAP_REQHDR_SIZE == rkbuf->rkbuf_totlen) {
                /* The full request is now read into the buffer. */
                rd_kafkap_str_t clientid;

                /* Set up response reader slice starting past the
                 * request header */
                rd_slice_init(&rkbuf->rkbuf_reader, &rkbuf->rkbuf_buf,
                              RD_KAFKAP_REQHDR_SIZE,
                              rd_buf_len(&rkbuf->rkbuf_buf) -
                              RD_KAFKAP_REQHDR_SIZE);

                /* For convenience, shave of the ClientId */
                rd_kafka_buf_read_str(rkbuf, &clientid);

                /* Return the buffer to the caller */
                *rkbufp = rkbuf;
                mconn->rxbuf = NULL;
                return 1;
        }

        return 0;


 err_parse:
        return -1;
}


static int rd_kafka_mock_handle_ApiVersion (rd_kafka_mock_connection_t *mconn,
                                            rd_kafka_buf_t *rkbuf);


/**
 * @brief Write a MetadataResponse.Topics. entry to \p resp.
 *
 * @param mtopic may be NULL
 */
static void
rd_kafka_mock_buf_write_Metadata_Topic (rd_kafka_buf_t *resp,
                                        int16_t ApiVersion,
                                        const char *topic,
                                        const rd_kafka_mock_topic_t *mtopic,
                                        rd_kafka_resp_err_t err) {
        int i;

        /* Response: Topics.ErrorCode */
        rd_kafka_buf_write_i16(resp, err);
        /* Response: Topics.Name */
        rd_kafka_buf_write_str(resp, topic);
        /* Response: Topics.IsInternal */
        rd_kafka_buf_write_bool(resp, rd_false);
        /* Response: Topics.#Partitions */
        rd_kafka_buf_write_i32(resp, mtopic ? mtopic->partition_cnt : 0);

        for (i = 0 ; mtopic && i < mtopic->partition_cnt ; i++) {
                const rd_kafka_mock_partition_t *mpart =
                        &mtopic->partitions[i];
                int r;

                /* Response: ..Partitions.ErrorCode */
                rd_kafka_buf_write_i16(resp, 0);
                /* Response: ..Partitions.PartitionIndex */
                rd_kafka_buf_write_i32(resp, mpart->id);
                /* Response: ..Partitions.Leader */
                rd_kafka_buf_write_i32(resp,
                                       mpart->leader ?
                                       mpart->leader->id : -1);

                if (rkbuf->rkbuf_reqhdr.ApiVersion >= 7) {
                        /* Response: ..Partitions.LeaderEpoch */
                        rd_kafka_buf_write_i32(resp, -1);
                }

                /* Response: ..Partitions.#ReplicaNodes */
                rd_kafka_buf_write_i32(resp,
                                       mpart->replica_cnt);
                for (r = 0 ; r < mpart->replica_cnt ; r++)
                        rd_kafka_buf_write_i32(
                                resp,
                                mpart->replicas[r]->id);

                /* Response: ..Partitions.#IsrNodes */
                /* Let Replicas == ISRs for now */
                rd_kafka_buf_write_i32(resp,
                                       mpart->replica_cnt);
                for (r = 0 ; r < mpart->replica_cnt ; r++)
                        rd_kafka_buf_write_i32(
                                resp,
                                mpart->replicas[r]->id);

                if (rkbuf->rkbuf_Reqhdr.ApiVersion >= 5) {
                        /* Response: ...OfflineReplicas */
                        rd_kafka_buf_write_i32(resp, 0);
                }
        }
}


/**
 * @brief Handle MetadataRequest
 */
static int rd_kafka_mock_handle_Metadata (rd_kafka_mock_connection_t *mconn,
                                          rd_kafka_buf_t *rkbuf) {
        rd_kafka_mock_cluster_t *mcluster = mconn->broker->cluster;
        int32_t TopicsCnt;
        int i;
        rd_bool_t AllowAutoTopicCreation = rd_true;
        rd_kafka_buf_t *resp = rd_kafka_mock_buf_new_response(rkbuf);
        const rd_kafka_mock_broker_t *mrkb;
        rd_kafka_topic_partition_list_t *requested_topics = NULL;
        rd_bool_t list_all_topics = rd_false;
        rd_kafka_mock_topic_t *mtopic;
        size_t of_TopicsCnt;

        /* Response: ThrottleTime */
        rd_kafka_buf_write_i32(resp, 0);

        /* Response: #Brokers */
        rd_kafka_buf_write_i32(resp, mcluster->broker_cnt);

        TAILQ_FOREACH(mrkb, &mcluster->brokers, link) {
                /* Response: Brokers.Nodeid */
                rd_kafka_buf_write_i32(resp, mrkb->id);
                /* Response: Brokers.Host */
                rd_kafka_buf_write_str(resp, mrkb->advertised_hostname);
                /* Response: Brokers.Port */
                rd_kafka_buf_write_i32(resp, mrkb->port);
                if (rkbuf->rkbuf_reqhdr.ApiVersion >= 1) {
                        /* Response: Brokers.Rack (Matt's going to love this) */
                        rd_kafka_buf_write_str(resp, NULL);
                }
        }

        /* Response: ClusterId */
        rd_kafka_buf_write_str(resp, mcluster->id);
        /* Response: ControllerId */
        rd_kafka_buf_write_i32(resp, mcluster->controller_id);

        /* #Topics */
        rd_kafka_buf_read_i32(rkbuf, &TopicsCnt);

        if (TopicsCnt > 0)
                requested_topics = rd_kafka_topic_partition_list_new(TopicsCnt);
        else if (rkbuf->rkbuf_reqhdr.ApiVersion == 0 || TopicsCnt == -1)
                list_all_topics = rd_true;

        for (i = 0 ; i < TopicsCnt ; i++) {
                rd_kafkap_str_t Topic;
                char *topic;
                rd_kafka_resp_err_t err;

                rd_kafka_buf_read_str(rkbuf, &Topic);
                RD_KAFKAP_STR_DUPA(topic, &Topic);

                rd_kafka_topic_partition_list_add(requested_topics, topic,
                                                  RD_KAFKA_PARTITION_UA);
        }

        if (rkbuf->rkbuf_reqhdr.ApiVersion >= 4)
                rd_kafka_buf_read_bool(rkbuf, &AllowAutoTopicCreation);

        if (rkbuf->rkbuf_reqhdr.ApiVersion >= 8) {
                rd_bool_t IncludeClusterAuthorizedOperations;
                rd_bool_t IncludeTopicAuthorizedOperations;
                rd_kafka_buf_read_bool(rkbuf,
                                       &IncludeClusterAuthorizedOperations);
                rd_kafka_buf_read_bool(rkbuf,
                                       &IncludeTopicAuthorizedOperations);
        }


        if (list_all_topics) {
                /* Response: #Topics */
                of_TopicsCnt = rd_kafka_buf_write_i32(resp,
                                                      mcluster->topic_cnt);

                TAILQ_FOREACH(mtopic, &mcluster->topics, link) {
                        rd_kafka_mock_buf_write_Metadata_Topic(
                                resp, rkbuf->rkbuf_reqhdr.ApiVersion,
                                mtopic->name, mtopic,
                                RD_KAFKA_RESP_ERR_NO_ERROR);
                }

        } else if (requested_topics) {
                /* Response: #Topics */
                of_TopicsCnt = rd_kafka_buf_write_i32(resp,
                                                      requested_topics->cnt);

                for (i = 0 ; i < requested_topics->cnt ; i++) {
                        const rd_kafka_topic_partition_t *rktpar =
                                &requested_topics->elems[i];
                        rd_kafka_mock_topic_t *mtopic;
                        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;

                        mtopic = rd_kafka_mock_topic_find(mcluster,
                                                          rktpar->topic);
                        if (!mtopic && AllowAutoTopicCreation)
                                mtopic = rd_kafka_mock_topic_auto_create(
                                        mcluster, rktpar->topic, &err);
                        else if (!mtopic)
                                err = RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART;

                        rd_kafka_mock_buf_write_Metadata_Topic(
                                resp, rkbuf->rkbuf_reqhdr.ApiVersion,
                                rktpar->topic, mtopic, err);
                }

                if (rkbuf->rkbuf_reqhdr.ApiVersion >= 8) {
                        /* TopicAuthorizedOperations */
                        rd_kafka_buf_write_i32(resp, -2147483648);
                }
        }

        if (rkbuf->rkbuf_reqhdr.ApiVersion >= 8) {
                /* ClusterAuthorizedOperations */
                rd_kafka_buf_write_i32(resp, -2147483648);
        }


        rd_kafka_mock_connection_send_response(mconn, rkbuf, resp);

        return 0;

 err_parse:
        if (requested_topics)
                rd_kafka_topic_partition_list_destroy(requested_topics);

        rd_kafka_buf_destroy(resp);
        return -1;

}

/**
 * @brief Default request handlers
 */
static const struct {
        int16_t MinVersion;
        int16_t MaxVersion;
        int (*cb) (rd_kafka_mock_connection_t *mconn, rd_kafka_buf_t *rkbuf);
} rd_kafka_mock_api_handlers[RD_KAFKAP__NUM] = {
        [RD_KAFKAP_ApiVersion] = { 0, 2, rd_kafka_mock_handle_ApiVersion },
        [RD_KAFKAP_Metadata] = { 0, 2, rd_kafka_mock_handle_Metadata },
};


static rd_kafka_buf_t *
rd_kafka_mock_buf_new_response (const rd_kafka_buf_t *request) {
        rd_kafka_buf_t *rkbuf = rd_kafka_buf_new(1, 100);

        /* Size, updated later */
        rd_kafka_buf_write_i32(rkbuf, 0);

        /* CorrId */
        rd_kafka_buf_write_i32(rkbuf, request->rkbuf_reqhdr.CorrId);

        return rkbuf;
}


static int rd_kafka_mock_handle_ApiVersion (rd_kafka_mock_connection_t *mconn,
                                            rd_kafka_buf_t *rkbuf) {
        rd_kafka_buf_t *resp = rd_kafka_mock_buf_new_response(rkbuf);
        size_t of_ApiKeysCnt;
        int cnt = 0;
        int i;

        /* ErrorCode */
        rd_kafka_buf_write_i16(resp, RD_KAFKA_RESP_ERR_NO_ERROR);

        /* #ApiKeys */
        of_ApiKeysCnt = rd_kafka_buf_write_i32(resp, 0); /* updated later */

        for (i = 0 ; i < RD_KAFKAP__NUM ; i++) {
                if (!rd_kafka_mock_api_handlers[i].cb)
                        continue;

                /* ApiKey */
                rd_kafka_buf_write_i16(resp, (int16_t)i);
                /* MinVersion */
                rd_kafka_buf_write_i16(
                        resp, rd_kafka_mock_api_handlers[i].MinVersion);
                /* MaxVersion */
                rd_kafka_buf_write_i16(
                        resp, rd_kafka_mock_api_handlers[i].MaxVersion);

                cnt++;
        }

        rd_kafka_buf_update_i32(resp, of_ApiKeysCnt, cnt);

        if (rkbuf->rkbuf_reqhdr.ApiVersion >= 1) {
                /* ThrottletimeMs */
                rd_kafka_buf_write_i32(resp, 0);
        }

        rd_kafka_mock_connection_send_response(mconn, rkbuf, resp);

        return 0;
}


/**
 * @brief Parse protocol request.
 *
 * @returns 0 on success, -1 on parse error.
 */
static int
rd_kafka_mock_connection_parse_request (rd_kafka_mock_connection_t *mconn,
                                        rd_kafka_buf_t *rkbuf) {
        rd_kafka_t *rk = mconn->broker->cluster->rk;

        if (rkbuf->rkbuf_reqhdr.ApiKey < 0 ||
            rkbuf->rkbuf_reqhdr.ApiKey >= RD_KAFKAP__NUM ||
            !rd_kafka_mock_api_handlers[rkbuf->rkbuf_reqhdr.ApiKey].cb) {
                rd_kafka_log(rk, LOG_ERR, "MOCK",
                             "Mock broker %"PRId32": unsupported %sRequestV%hd "
                             "from %s",
                             mconn->broker->id,
                             rd_kafka_ApiKey2str(rkbuf->rkbuf_reqhdr.ApiKey),
                             rkbuf->rkbuf_reqhdr.ApiVersion,
                             rd_sockaddr2str(&mconn->peer,
                                             RD_SOCKADDR2STR_F_PORT));
                return -1;
        }

        if (rkbuf->rkbuf_reqhdr.ApiVersion <
            rd_kafka_mock_api_handlers[rkbuf->rkbuf_reqhdr.ApiKey].MinVersion ||
            rkbuf->rkbuf_reqhdr.ApiVersion >
            rd_kafka_mock_api_handlers[rkbuf->rkbuf_reqhdr.ApiKey].MaxVersion) {
                rd_kafka_log(rk, LOG_ERR, "MOCK",
                             "Mock broker %"PRId32": unsupported %sRequest "
                             "version %hd from %s",
                             mconn->broker->id,
                             rd_kafka_ApiKey2str(rkbuf->rkbuf_reqhdr.ApiKey),
                             rkbuf->rkbuf_reqhdr.ApiVersion,
                             rd_sockaddr2str(&mconn->peer,
                                             RD_SOCKADDR2STR_F_PORT));
                return -1;
        }
        rd_kafka_dbg(rk, MOCK, "MOCK",
                     "Broker %"PRId32": Received %sRequestV%hd from %s",
                     mconn->broker->id,
                     rd_kafka_ApiKey2str(rkbuf->rkbuf_reqhdr.ApiKey),
                     rkbuf->rkbuf_reqhdr.ApiVersion,
                     rd_sockaddr2str(&mconn->peer, RD_SOCKADDR2STR_F_PORT));

        return rd_kafka_mock_api_handlers[rkbuf->rkbuf_reqhdr.ApiKey].cb(mconn,
                                                                         rkbuf);
}



/**
 * @brief Send as many bytes as possible from the output buffer.
 *
 * @returns 1 if all buffers were sent, 0 if more buffers need to be sent, or
 *          -1 on error.
 */
static ssize_t
rd_kafka_mock_connection_write_out (rd_kafka_mock_connection_t *mconn) {
        rd_kafka_buf_t *rkbuf;

        while ((rkbuf = TAILQ_FIRST(&mconn->outbufs.rkbq_bufs))) {
                ssize_t r;
                char errstr[128];

                if ((r = rd_kafka_transport_send(mconn->transport,
                                                 &rkbuf->rkbuf_reader,
                                                 errstr,
                                                 sizeof(errstr))) == -1)
                        return -1;

                rd_kafka_dbg(mconn->broker->cluster->rk, MOCK, "MOCK",
                             "Wrote %"PRIdsz" bytes, %"PRIusz" remains",
                             r, rd_slice_remains(&rkbuf->rkbuf_reader));
                if (rd_slice_remains(&rkbuf->rkbuf_reader) > 0)
                        return 0; /* Partial send, continue next time */

                /* Entire buffer sent, unlink and free */
                rd_kafka_bufq_deq(&mconn->outbufs, rkbuf);

                rd_kafka_buf_destroy(rkbuf);
        }

        *mconn->poll_events &= ~POLLOUT;

        return 1;
}



/**
 * @brief Per-Connection IO handler
 */
static void rd_kafka_mock_connection_io (rd_kafka_mock_cluster_t *mcluster,
                                         int fd, int events, void *opaque) {
        rd_kafka_mock_connection_t *mconn = opaque;

        if (events & POLLIN) {
                rd_kafka_buf_t *rkbuf;
                int r;

                while (1) {
                        /* Read full request */
                        r = rd_kafka_mock_connection_read_request(mconn,
                                                                  &rkbuf);
                        if (r == 0)
                                break; /* Need more data */
                        else if (r == -1) {
                                rd_kafka_mock_connection_close(mconn,
                                                               "Read error");
                                return;
                        }

                        /* Parse and handle request */
                        r = rd_kafka_mock_connection_parse_request(mconn,
                                                                   rkbuf);
                        rd_kafka_buf_destroy(rkbuf);
                        if (r == -1) {
                                rd_kafka_mock_connection_close(mconn,
                                                               "Parse error");
                                return;
                        }
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
        rd_kafka_transport_t *rktrans;
        char errstr[128];

        rktrans = rd_kafka_transport_new(mrkb->cluster->dummy_rkb, s,
                                         errstr, sizeof(errstr));
        if (!rktrans) {
                rd_kafka_log(mrkb->cluster->rk, LOG_ERR, "MOCK",
                             "Failed to create transport for new "
                             "mock connection: %s", errstr);
                return NULL;
        }

        rd_kafka_transport_post_connect_setup(rktrans);

        mconn = rd_calloc(1, sizeof(*mconn));
        mconn->broker = mrkb;
        mconn->transport = rktrans;
        mconn->peer = *peer;
        rd_kafka_bufq_init(&mconn->outbufs);

        TAILQ_INSERT_TAIL(&mrkb->connections, mconn, link);

        mconn->poll_events =
                rd_kafka_mock_cluster_io_add(mrkb->cluster,
                                             mconn->transport->rktrans_s,
                                             POLLIN,
                                             rd_kafka_mock_connection_io,
                                             mconn);

        rd_kafka_dbg(mrkb->cluster->rk, MOCK, "MOCK",
                     "New connection to mock broker %"PRId32" from %s",
                     mrkb->id,
                     rd_sockaddr2str(&mconn->peer, RD_SOCKADDR2STR_F_PORT));

        return mconn;
}



static void rd_kafka_mock_cluster_ctrl_io (rd_kafka_mock_cluster_t *mcluster,
                                           int fd, int events, void *opaque) {
        rd_kafka_dbg(mcluster->rk, MOCK, "MOCK", "Shutting down mock cluster");
        mcluster->run = rd_false;
}


static int rd_kafka_mock_cluster_io_poll (rd_kafka_mock_cluster_t *mcluster) {
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
                return -1;
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

        return 0;
}


static int rd_kafka_mock_cluster_thread_main (void *arg) {
        rd_kafka_mock_cluster_t *mcluster = arg;

        rd_kafka_set_thread_name("mock");
        rd_kafka_set_thread_sysname("rdk:mock");
        rd_kafka_interceptors_on_thread_start(mcluster->rk,
                                              RD_KAFKA_THREAD_BACKGROUND);
        rd_atomic32_add(&rd_kafka_thread_cnt_curr, 1);

        /* Control socket IO */
        rd_kafka_mock_cluster_io_add(mcluster, mcluster->ctrl_s[1],
                                     POLLIN|POLLHUP,
                                     rd_kafka_mock_cluster_ctrl_io, NULL);

        mcluster->run = rd_true;

        while (mcluster->run) {

                if (rd_kafka_mock_cluster_io_poll(mcluster) == -1)
                        break;
        }

        rd_kafka_mock_cluster_io_del(mcluster, mcluster->ctrl_s[1]);


        rd_kafka_interceptors_on_thread_exit(mcluster->rk,
                                             RD_KAFKA_THREAD_BACKGROUND);
        rd_atomic32_sub(&rd_kafka_thread_cnt_curr, 1);

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
        mrkb->port = ntohs(sin.sin_port);
        rd_snprintf(mrkb->advertised_listener,
                    sizeof(mrkb->advertised_listener),
                    "%s", rd_sockaddr2str(&sin, 0));

        TAILQ_INIT(&mrkb->connections);

        TAILQ_INSERT_TAIL(&mcluster->brokers, mrkb, link);
        mcluster->broker_cnt++;

        rd_kafka_mock_cluster_io_add(mcluster, listen_s, POLLIN,
                                     rd_kafka_mock_broker_listen_io, mrkb);

        return mrkb;
}



static void
rd_kafka_mock_cluster_destroy0 (rd_kafka_mock_cluster_t *mcluster) {
        rd_kafka_mock_broker_t *mrkb;

        while ((mrkb = TAILQ_FIRST(&mcluster->brokers)))
                rd_kafka_mock_broker_destroy(mrkb);

        rd_kafka_broker_destroy(mcluster->dummy_rkb);

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


/**
 * @brief Start the mock cluster
 */
void rd_kafka_mock_cluster_start (rd_kafka_mock_cluster_t *mcluster) {

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
        mcluster->dummy_rkb = rd_kafka_broker_add(rk, RD_KAFKA_INTERNAL,
                                                  RD_KAFKA_PROTO_PLAINTEXT,
                                                  "mock", 0,
                                                  RD_KAFKA_NODEID_UA);
        rd_snprintf(mcluster->id, sizeof(mcluster->id),
                    "mockCluster%x", (int)rk ^ (int)mcluster);

        TAILQ_INIT(&mcluster->brokers);
        TAILQ_INIT(&mcluster->topics);

        for (i = 1 ; i <= broker_cnt ; i++) {
                if (!(mrkb = rd_kafka_mock_broker_new(mcluster, i))) {
                        rd_kafka_mock_cluster_destroy(mcluster);
                        return NULL;
                }

                /* advertised listener + ":port" + "," */
                bootstraps_len += strlen(mrkb->advertised_listener) + 6 + 1;
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
                int r;
                r = rd_snprintf(&mcluster->bootstraps[of],
                                bootstraps_len - of,
                                "%s%s:%d",
                                of > 0 ? "," : "",
                                mrkb->advertised_listener, mrkb->port);
                of += r;
                rd_assert(of < bootstraps_len);
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
