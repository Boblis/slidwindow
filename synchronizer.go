package slidingwindow

import (
	"log"
	"time"
)

type Datastore interface {
	Add(key string, start, delta int64) (int64, error)

	Get(key string, start int64) (int64, error)
}

type syncHelper struct {
	store        Datastore
	syncInterval time.Duration

	inProgress bool // Whether the synchronization is in progress.
	lastSynced time.Time
}

func newSyncHelper(store Datastore, syncInterval time.Duration) *syncHelper {
	return &syncHelper{store: store, syncInterval: syncInterval}
}

func (h *syncHelper) IsTimeUp(now time.Time) bool {
	return !h.inProgress && now.Sub(h.lastSynced) >= h.syncInterval
}

func (h *syncHelper) InProgress() bool {
	return h.inProgress
}

func (h *syncHelper) Begin(now time.Time) {
	h.inProgress = true
	h.lastSynced = now
}

func (h *syncHelper) End() {
	h.inProgress = false
}

func (h *syncHelper) Sync(req SyncRequest) (resp SyncResponse, err error) {
	var newCount int64

	if req.Changes > 0 {
		newCount, err = h.store.Add(req.Key, req.Start, req.Changes)
	} else {
		newCount, err = h.store.Get(req.Key, req.Start)
	}

	if err != nil {
		return SyncResponse{}, err
	}

	return SyncResponse{
		OK:           true,
		Start:        req.Start,
		Changes:      req.Changes,
		OtherChanges: newCount - req.Count,
	}, nil
}

type BlockingSynchronizer struct {
	helper *syncHelper
}

func NewBlockingSynchronizer(store Datastore, syncInterval time.Duration) *BlockingSynchronizer {
	return &BlockingSynchronizer{
		helper: newSyncHelper(store, syncInterval),
	}
}

func (s *BlockingSynchronizer) Start() {}

func (s *BlockingSynchronizer) Stop() {}

func (s *BlockingSynchronizer) Sync(now time.Time, makeReq MakeFunc, handleResp HandleFunc) {
	if s.helper.IsTimeUp(now) {
		s.helper.Begin(now)

		resp, err := s.helper.Sync(makeReq())
		if err != nil {
			log.Printf("err: %v\n", err)
		}

		handleResp(resp)
		s.helper.End()
	}
}

type NonblockingSynchronizer struct {
	reqC  chan SyncRequest
	respC chan SyncResponse

	stopC chan struct{}
	exitC chan struct{}

	helper *syncHelper
}

func NewNonblockingSynchronizer(store Datastore, syncInterval time.Duration) *NonblockingSynchronizer {
	return &NonblockingSynchronizer{
		reqC:   make(chan SyncRequest),
		respC:  make(chan SyncResponse),
		stopC:  make(chan struct{}),
		exitC:  make(chan struct{}),
		helper: newSyncHelper(store, syncInterval),
	}
}

func (s *NonblockingSynchronizer) Start() {
	go s.syncLoop()
}

func (s *NonblockingSynchronizer) Stop() {
	close(s.stopC)
	<-s.exitC
}

func (s *NonblockingSynchronizer) syncLoop() {
	for {
		select {
		case req := <-s.reqC:
			resp, err := s.helper.Sync(req)
			if err != nil {
				log.Printf("err: %v\n", err)
			}

			select {
			case s.respC <- resp:
			case <-s.stopC:
				goto exit
			}
		case <-s.stopC:
			goto exit
		}
	}

exit:
	close(s.exitC)
}

func (s *NonblockingSynchronizer) Sync(now time.Time, makeReq MakeFunc, handleResp HandleFunc) {
	if s.helper.IsTimeUp(now) {
		select {
		case s.reqC <- makeReq():
			s.helper.Begin(now)
		default:
		}
	}

	if s.helper.InProgress() {
		select {
		case resp := <-s.respC:
			handleResp(resp)
			s.helper.End()
		default:
		}
	}
}
