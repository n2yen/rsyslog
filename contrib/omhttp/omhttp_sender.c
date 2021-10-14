#include "rsyslog.h"
#include "errmsg.h"
#include <errno.h>
#include <sys/queue.h>
#include <curl/curl.h>
#include "omhttp_sender.h"
#include "module-template.h"
#include "glbl.h"
#include <apr_pools.h>

DEFobjCurrIf(glbl)

static rsRetVal
initIoQ(sender_q_t *sender_q, size_t capacity)
{
	DEFiRet;
	CHKiConcCtrl(pthread_mutex_init(&sender_q->mut, NULL));
	CHKiConcCtrl(pthread_cond_init(&sender_q->wakeup_worker, NULL));
	CHKiConcCtrl(pthread_cond_init(&sender_q->cond_has_space, NULL));
	STAILQ_INIT(&sender_q->head);
	sender_q->size = 0;
	sender_q->capacity = capacity; /* TODO: discuss this and fix potential concurrent read/write issues */
finalize_it:
	RETiRet;
}

static void destroyIoQ(sender_q_t *sender_q)
{
	sender_req_t *req;
#if 0
	if (io_q.stats != NULL) {
		statsobj.Destruct(&io_q.stats);
	}
#endif
	pthread_mutex_lock(&sender_q->mut);
	while (!STAILQ_EMPTY(&sender_q->head)) {
		req = STAILQ_FIRST(&sender_q->head);
		STAILQ_REMOVE_HEAD(&sender_q->head, link);
#if 0
		LogError(0, RS_RET_INTERNAL_ERROR, "imptcp: discarded enqueued io-work to allow shutdown "
								"- ignored");
#else
		printf("omhttp-sender: discarded enqueued io-work to allow shutdown "
				"- ignored\n");

#endif
		free(req);
	}
	sender_q->size = 0;
	pthread_mutex_unlock(&sender_q->mut);
	pthread_cond_destroy(&sender_q->wakeup_worker);
	pthread_cond_destroy(&sender_q->cond_has_space);
	pthread_mutex_destroy(&sender_q->mut);
}

rsRetVal
enqueueSendReq2(sender_t *sender, omhttp_request_data_t *pRequestData)
{
	DEFiRet;
	apr_status_t apr_rv = APR_SUCCESS;
	int i = 0,
		max_tries = 10;

	while (i++ < max_tries) {
		apr_rv = apr_queue_push(sender->request_q, (void*)pRequestData);
		if (apr_rv == APR_SUCCESS) {
			printf("pushed into queue postdata: %s\n", pRequestData->postData);
			break;
		} else if (apr_rv == APR_EINTR) {
			// retry
			printf("enqueue EINTR: %d\n", apr_rv);
			continue;
		} else if (apr_rv == APR_EOF) {
			// queue is terminated we should be shutting down.
			printf("enqueue EOF: %d\n", apr_rv);
			ABORT_FINALIZE(iRet = RS_RET_SUSPENDED);
			break;
		} else {
			ABORT_FINALIZE(iRet = RS_RET_SUSPENDED);
		}
	}
	assert(i < max_tries);

finalize_it:
	if (apr_rv != APR_SUCCESS) {
		char buf[256];
		printf("omhttp-sender: enqueue failed - iterations: %d, error: %d, %s\n", apr_rv, apr_strerror(apr_rv, buf, sizeof(buf)));
		iRet = RS_RET_OUT_OF_MEMORY;
		//assert(0);
	}
	RETiRet;
}

rsRetVal
enqueueSendReq(sender_q_t *sender_q, omhttp_request_data_t *pRequestData)
{
	sender_req_t *req;
	DEFiRet;

	CHKmalloc(req = malloc(sizeof(sender_req_t)));
	req->pRequestData = pRequestData;
	pthread_mutex_lock(&sender_q->mut);
/*
	if (dispatchInlineIfQueueFull && io_q.sz > inlineDispatchThreshold) {
		dispatchInline = 1;
	} else {
*/
	printf("enqueue waiting for room... pRequestData: %p\n", (void*)req->pRequestData);
	while(sender_q->size >= sender_q->capacity) {
		printf("producer(%p) - size: %d, capacity: %d, waiting for room...\n",
				(void*)pthread_self(), sender_q->size, sender_q->capacity);
		assert(sender_q->size >= sender_q->capacity);
		pthread_cond_wait(&sender_q->cond_has_space, &sender_q->mut);
	}

	STAILQ_INSERT_TAIL(&sender_q->head, req, link);
	sender_q->size++;
	#if 0
	STATSCOUNTER_INC(sender_q->ctrEnq, sender_q.mutCtrEnq);
	STATSCOUNTER_SETMAX_NOMUT(sender_q->ctrMaxSz, sender_q->sz);
	#endif
	printf("inserted requestData: %p\n", (void*)req->pRequestData);
	//pthread_cond_signal(&sender_q->wakeup_worker);
#if 0
	else {
		iRet = RS_RET_SUSPENDED;
		printf("we are beyond capacity, let's suspend.\n");
	}
#endif
	pthread_mutex_unlock(&sender_q->mut);

finalize_it:
	if (iRet != RS_RET_OK) {
		if (req == NULL) {
			#if 0
			LogError(0, iRet, "imptcp: couldn't allocate memory to enqueue io-request - ignored");
			#else
			printf("omhttp: couldn't allocate memory to enqueue io-request - ignored\n");
			#endif
		}
	}
	RETiRet;
}

#if 0
static consumeRemainingRequests()
{
		if (me->runstate == 1 )
		{
			pthread_mutex_lock(&me->sender_q.mut);
			size_t cur_size = me->sender_q.size;
			pthread_mutex_unlock(&me->sender_q.mut);
			printf(">>>> queue is shutting down!!! - items left: %d\n", cur_size);
			if (cur_size > 0) {
				// consume the rest
				for (int i = 0; i < cur_size; ++i) {
					req = dequeueSendReq(&me->sender_q);
					if (req) {
						printf("request taken: requestData: %p\n", (void*)req->pRequestData);
					}
				}
			}
		}
}
#endif

static rsRetVal dequeueSendReq2(apr_queue_t *queue, omhttp_request_data_t **pRequestDataOut)
{
	DEFiRet;
	apr_status_t apr_rv;
	int i = 0,
		max_tries = 3;
	omhttp_request_data_t *pdata;

	while (i++ < max_tries) {
		apr_rv = apr_queue_trypop(queue, &pdata);
		if (apr_rv == APR_SUCCESS) {
			// done
			*pRequestDataOut = pdata;
			printf("dequeued postdata: %s\n", (*pRequestDataOut)->postData);
			break;
		} else if (apr_rv == APR_EINTR) {
			// retry
			continue;
		} else if (apr_rv == APR_EOF) {
			// queue shutdown
			RS_RET_SENDER_GONE_AWAY;
			break;
		} else {
			// unexpected
			ABORT_FINALIZE(RS_RET_ERR);
		}
	}
finalize_it:
	RETiRet;
}

static rsRetVal dequeueSendReq(sender_q_t *sender_q, omhttp_request_data_t **pRequestDataOut)
{
	sender_req_t *req = NULL;

	pthread_mutex_lock(&sender_q->mut);

	// Note: this may not be needed, as we don't really want to block.
	// we want this consumption of this queue to be driven by available
	// connections.
	//pthread_cond_wait(&sender_q->wakeup_worker, &sender_q->mut);

	if (sender_q->size > 0) {
		req = STAILQ_FIRST(&sender_q->head);
		STAILQ_REMOVE_HEAD(&sender_q->head, link);
		sender_q->size--;
		printf("dequeued requestdata: %p\n", req->pRequestData);
		*pRequestDataOut = req->pRequestData;
		free(req);
		pthread_cond_signal(&(sender_q->cond_has_space));
		printf("signalling there's room - size: %d\n", sender_q->size);
	}
	pthread_mutex_unlock(&sender_q->mut);

	return 0;
}
#define WAITMS(x) \
    struct timeval wait = { 0, (x)*1000 }; \
    (void)select(0, NULL, NULL, NULL, &wait);

static __attribute__((noreturn)) void *sender_task(void *data)
{
	sender_t* me = (sender_t*) data;
	//int still_running = 0;
	int i = 0;
	int max = 10;
	int numfds = 0;

	sleep(1);
	printf("thread: (%p) started.\n", (const void*)me->tid);
	assert(me->tid == pthread_self());
	int still_running = 0;
	int count= 0;
	int repeats = 0;

	while (1)
	{
		int res = CURLM_OK;
		// get as long as there are items in
		for (int i = 0; i < me->n_curl_handles; ++i)
		//for (int i = 0; i < 1; ++i)
		{
			omhttp_request_data_t *pRequestData = NULL;
#if 1
			dequeueSendReq2(me->request_q, &pRequestData);
#else
			dequeueSendReq(&me->sender_q, &pRequestData);
#endif
			if (pRequestData) {
#if 1
				CURL *curl_h = curl_easy_init();
#else
				CURL *curl_h = me->curl_handles[i];
				curl_easy_reset(curl_h);
#endif
				assert(curl_h != NULL);
				if (me->curl_setup) {
					me->curl_setup(curl_h, pRequestData);
				}
				//printf("request taken: requestData: %p\n", (void *)pRequestData);
				CURLMcode mcode = curl_multi_add_handle(me->curlm, curl_h);
				if (mcode != CURLM_OK) {
					LogError(0, RS_RET_ERR,
									 "omhttp_sender: error curl_multi_add_handle ret- %d:%s\n",
									 mcode, curl_multi_strerror(mcode));
					assert(0);
				}
			} else {
				printf("empty request.\n");
				break;
			}
		}

		{
			int prev_still_running = still_running;
			CURLMcode mc = curl_multi_perform(me->curlm, &still_running);
			int repeats = 0;
			do {
				int numfds;
				printf("calling curl_multi_wait()...\n");
				int res = curl_multi_wait(me->curlm, NULL, 0, 500, &numfds);
				printf("woke up.\n");
				if (res != CURLM_OK) {
					fprintf(stderr, "error: curl_multi_wait() returned %d\n", res);
					break;
				}
				mc = curl_multi_perform(me->curlm, &still_running);
				if (mc != CURLM_OK) {
					fprintf(stderr, "curl_multi failed, code %d\n", mc);
					break;
				}
				printf("still_running: %d\n", still_running);
				if (!numfds) {
					repeats++;
					if (repeats > 1) {
						WAITMS(100);
					}
				} else {
					repeats = 0;
				}
			} while (still_running);

			{
				int msgs_left = 0;
				CURLMsg *msg = NULL;
				CURL *pCurl = NULL;
				int rc = 0;

				while ((msg = curl_multi_info_read(me->curlm, &msgs_left))) {
					if (msg->msg == CURLMSG_DONE) {
						pCurl = msg->easy_handle;
						rc = msg->data.result;
						if (rc != CURLE_OK) {
							LogError(0, RS_RET_ERR,
											 "omhttp: %s() - curl handle: %p, error code: %d:%s\n",
											 __FUNCTION__, (void *)pCurl, rc,
											 curl_multi_strerror(rc));
							// assert(0);
							continue;
						}

						CURLcode ccode;
						int lDebug = 1;
						if (lDebug) {
							long http_status = 0;
							curl_easy_getinfo(pCurl, CURLINFO_RESPONSE_CODE, &http_status);
							DBGPRINTF("http status: %lu\n", http_status);
							printf("http status: %lu\n", http_status);
							if (http_status == 200) {
								count++;
							}
						}
						curl_multi_remove_handle(me->curlm, pCurl);
#if 1
						curl_easy_cleanup(pCurl);
#else
						//me->curl_complete(pCurl);
						//curl_easy_reset(pCurl);
						me->curl_complete(pCurl);
#endif
					}
				}
			}
		}
		printf("iteration %d, 2xx responses: %d\n", ++i, count);
	}
	destroyIoQ(&me->sender_q);

	printf("total successful 200s responses: %d\n", count);
	printf("exiting thread.\n");
	pthread_exit(0);
}

static void init_apr(sender_t *sender) {
	if (apr_initialize() != APR_SUCCESS) {
		abort();
	}
	atexit(apr_terminate);

	apr_pool_create(&sender->_pool, NULL);
	apr_pool_tag(sender->_pool, "apr-util omhttp pool");
}

rsRetVal init_sender(sender_t *sender, size_t capacity, curl_setup_cb setup_cb, curl_complete_cb complete_cb)
{
	sender->tid = NULL;
	sender->curlm = curl_multi_init();
	sender->curl_handles = NULL;
	sender->n_curl_handles = 0;
	sender->curl_handles = calloc(capacity, sizeof(CURL*));
	sender->n_curl_handles = capacity;

	sender->runstate = 0;
	sender->curl_setup = setup_cb;
	sender->curl_complete = complete_cb;
	initIoQ(&sender->sender_q, capacity);
	/* apr related stuff */
	init_apr(sender);
	apr_status_t apr_rv = apr_queue_create(&sender->request_q, capacity, sender->_pool);
	assert(apr_rv == APR_SUCCESS);

	for (int i = 0; i < sender->n_curl_handles; ++i) {
		sender->curl_handles[i] = curl_easy_init();
	}
}

void start_send_worker(sender_t *sender) {
	printf("starting worker thread...\n");
	pthread_create(&sender->tid, NULL, sender_task, sender);
}

void stop_send_worker(sender_t *sender) {
	printf("stopping worker thread...\n");
	sender->runstate = 1;
}
// End new multi-threaded sender interface
