#include "rsyslog.h"
#include "errmsg.h"
#include <errno.h>
#include <sys/queue.h>
#include <curl/curl.h>
#include "omhttp_sender.h"
#include "module-template.h"
#include "glbl.h"

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
enqueueSendReq(sender_q_t *sender_q, CURL* curl, const void* private_data)
{
	sender_req_t *req;
	DEFiRet;

	CHKmalloc(req = malloc(sizeof(sender_req_t)));
	req->curl_h = curl;
	req->buffer= NULL;
	req->private_data = private_data;
	pthread_mutex_lock(&sender_q->mut);
/*
	if (dispatchInlineIfQueueFull && io_q.sz > inlineDispatchThreshold) {
		dispatchInline = 1;
	} else {
*/
	printf("enqueue waiting for room... curl: %p\n", (void*)req->curl_h);
	while(sender_q->size >= sender_q->capacity) {
		printf("producer(%p) - size: %d, capacity: %d, waiting for room...\n",
				(void*)pthread_self(), sender_q->size, sender_q->capacity);
		pthread_cond_wait(&sender_q->cond_has_space, &sender_q->mut);
	}
#if 0
	//if (sender_q->size < sender_q->capacity)
#endif
	{
		STAILQ_INSERT_TAIL(&sender_q->head, req, link);
		sender_q->size++;
		#if 0
		STATSCOUNTER_INC(sender_q->ctrEnq, sender_q.mutCtrEnq);
		STATSCOUNTER_SETMAX_NOMUT(sender_q->ctrMaxSz, sender_q->sz);
		#endif

		printf("inserted curl request: %p\n", (void*)req->curl_h);
		//pthread_cond_signal(&sender_q->wakeup_worker);
	}
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

static sender_req_t* dequeueSendReq(sender_q_t *sender_q)
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
		pthread_cond_signal(&(sender_q->cond_has_space));
		printf("signalling there's room - size: %d\n", sender_q->size);
	}
	pthread_mutex_unlock(&sender_q->mut);

	return req;
}


//static ATTR_NORETURN void *startCaptureThread(void *sender_data) {
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
		sender_req_t *req = NULL;

		if (me->runstate == 1 )
#if 1
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
						printf("request taken: curl_h: %p\n", (void*)req->curl_h);
					}
				}
			}
		}
#endif
		int res = CURLM_OK;
		req = NULL;

		// get as long as there are items in
		if (me->n_curl_handles < me->sender_q.capacity) {
			req = dequeueSendReq(&me->sender_q);
		}

		if (req) {
			printf("request taken: curl_h: %p\n", (void*)req->curl_h);
			/* this isn't thread safe */
			#if 1
			CURLMcode mcode = curl_multi_add_handle(me->curlm, req->curl_h);
			if (mcode != CURLM_OK) {
				LogError(0, RS_RET_ERR, "omhttp_sender: error curl_multi_add_handle ret- %d:%s\n",
					mcode, curl_multi_strerror(mcode));
				assert(0);
			}
			me->n_curl_handles++;
			#endif
		} else {
			;
			//printf("empty request.\n");
		}
		#if 1
		{
			CURLMcode mc = curl_multi_perform(me->curlm, &still_running);
			if (mc == CURLM_OK) {
				res = curl_multi_wait(me->curlm, NULL, 0, 1000, &numfds);
				if (res != CURLM_OK) {
					LogError(0, RS_RET_ERR, "error: curl_multi_wait() numfds=%d, res=%d:%s\n",
							numfds, res, curl_multi_strerror(res));
				}
				printf ("numfds returned: %d\n", numfds);
				#if 0
				if (!numfds) {
					repeats++; /* count number of repeated zero numfds */
					if (repeats > 1) {
						sleep(100); /* sleep 100 milliseconds */
					}
				} else {
					repeats = 0;
				}
				#endif
			} else {
				LogError(0, RS_RET_ERR, "error: curl_multi_perform() still_running=%d, res=%d:%s\n",
								 still_running, res, curl_multi_strerror(res));
			}

			int prev_still_running = still_running;
			//curl_multi_perform(me->curlm, &still_running);

			printf("numfds: %d, prev: %d, still %d\n", numfds, prev_still_running, still_running);
			//if (prev_still_running > still_running)
			for (int i = 0; i < numfds; ++i)
			{
				printf("checking responses...\n");
				//curl_multi_perform(me->curlm, &still_running);
				int rc = 0, msgs_left = 0;
				CURLMsg *msg = NULL;
				CURL *pCurl;

				while ((msg = curl_multi_info_read(me->curlm, &msgs_left))) {
					if (msg->msg == CURLMSG_DONE) {
						pCurl = msg->easy_handle;
						rc = msg->data.result;
						if (rc != CURLE_OK) {
							LogError(0, RS_RET_ERR, "omhttp: %s() - curl handle: %p, error code: %d:%s\n",
											 __FUNCTION__, (void*)pCurl, rc, curl_multi_strerror(rc));
							//assert(0);
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
						me->curl_complete(pCurl);
						//curl_easy_cleanup(pCurl);
						me->n_curl_handles--;
					}
				}
			}
		}
		#endif
		printf("iteration %d, 2xx responses: %d\n", ++i, count);
	}
	destroyIoQ(&me->sender_q);

	printf("total successful 200s responses: %d\n", count);
	printf("exiting thread.\n");
	pthread_exit(0);
}
#if 0
static rsRetVal
omhttp_sender_task() {
	int still_running = 0;

	do {
		int numfds = 0;
		int res = CURLM_OK;

		res = curl_multi_wait(curlm, fds, 0, 1, &ret);
		if (res != CURLM_OK) {
			LogError(0, RS_RET_ERR, "error: curl_multi_wait() numfds=%d, res=%d:%s\n",
					numfds, res, curl_multi_strerror(res));
		}

	} while (still_running);
}
#endif

//void init_sender(sender_t *sender, size_t capacity)
void init_sender(sender_t *sender, size_t capacity, curl_complete_cb cb)
{
	sender->tid = NULL;
	sender->curlm = curl_multi_init();
	sender->curl_handles = NULL;
	sender->n_curl_handles = 0;
	sender->runstate = 0;
	sender->curl_complete = cb;
	initIoQ(&sender->sender_q, capacity);
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
