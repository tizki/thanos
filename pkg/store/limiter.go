// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/alecthomas/units"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/atomic"

	"crypto/sha256"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type ChunksLimiter interface {
	// Reserve num chunks out of the total number of chunks enforced by the limiter.
	// Returns an error if the limit has been exceeded. This function must be
	// goroutine safe.
	Reserve(num uint64) error
}

type SeriesLimiter interface {
	// Reserve num series out of the total number of series enforced by the limiter.
	// Returns an error if the limit has been exceeded. This function must be
	// goroutine safe.
	Reserve(num uint64) error
}

type BytesLimiter interface {
	// Reserve bytes out of the total amount of bytes enforced by the limiter.
	// Returns an error if the limit has been exceeded. This function must be
	// goroutine safe.
	Reserve(num uint64) error
}

// ChunksLimiterFactory is used to create a new ChunksLimiter. The factory is useful for
// projects depending on Thanos (eg. Cortex) which have dynamic limits.
type ChunksLimiterFactory func(failedCounter prometheus.Counter) ChunksLimiter

// SeriesLimiterFactory is used to create a new SeriesLimiter.
type SeriesLimiterFactory func(failedCounter prometheus.Counter) SeriesLimiter

// BytesLimiterFactory is used to create a new BytesLimiter.
type BytesLimiterFactory func(failedCounter prometheus.Counter) BytesLimiter

// Limiter is a simple mechanism for checking if something has passed a certain threshold.
type Limiter struct {
	limit    uint64
	reserved atomic.Uint64

	// Counter metric which we will increase if limit is exceeded.
	failedCounter prometheus.Counter
	failedOnce    sync.Once
}

// NewLimiter returns a new limiter with a specified limit. 0 disables the limit.
func NewLimiter(limit uint64, ctr prometheus.Counter) *Limiter {
	return &Limiter{limit: limit, failedCounter: ctr}
}

// Reserve implements ChunksLimiter.
func (l *Limiter) Reserve(num uint64) error {
	if l == nil {
		return nil
	}
	if l.limit == 0 {
		return nil
	}
	reserved := l.reserved.Add(num)
	if reserved > l.limit {
		// We need to protect from the counter being incremented twice due to concurrency
		// while calling Reserve().
		l.failedOnce.Do(l.failedCounter.Inc)
		return errors.Errorf("limit %v violated (got %v)", l.limit, reserved)
	} else {
		fmt.Printf("Reserved %v chunks out of %v\n", l.reserved, l.limit)
	}
	return nil
}

// NewChunksLimiterFactory makes a new ChunksLimiterFactory with a static limit.
func NewChunksLimiterFactory(limit uint64) ChunksLimiterFactory {
	return func(failedCounter prometheus.Counter) ChunksLimiter {
		return NewLimiter(limit, failedCounter)
	}
}

// NewSeriesLimiterFactory makes a new SeriesLimiterFactory with a static limit.
func NewSeriesLimiterFactory(limit uint64) SeriesLimiterFactory {
	return func(failedCounter prometheus.Counter) SeriesLimiter {
		return NewLimiter(limit, failedCounter)
	}
}

// NewBytesLimiterFactory makes a new BytesLimiterFactory with a static limit.
func NewBytesLimiterFactory(limit units.Base2Bytes) BytesLimiterFactory {
	return func(failedCounter prometheus.Counter) BytesLimiter {
		return NewLimiter(uint64(limit), failedCounter)
	}
}

// SeriesSelectLimits are limits applied against individual Series calls.
type SeriesSelectLimits struct {
	SeriesPerRequest  uint64
	SamplesPerRequest uint64
}

func (l *SeriesSelectLimits) RegisterFlags(cmd extkingpin.FlagClause) {
	cmd.Flag("store.limits.request-series", "The maximum series allowed for a single Series request. The Series call fails if this limit is exceeded. 0 means no limit.").Default("0").Uint64Var(&l.SeriesPerRequest)
	cmd.Flag("store.limits.request-samples", "The maximum samples allowed for a single Series request, The Series call fails if this limit is exceeded. 0 means no limit. NOTE: For efficiency the limit is internally implemented as 'chunks limit' considering each chunk contains a maximum of 120 samples.").Default("0").Uint64Var(&l.SamplesPerRequest)
}

var _ storepb.StoreServer = &limitedStoreServer{}

// limitedStoreServer is a storepb.StoreServer that can apply series and sample limits against individual Series requests.
type limitedStoreServer struct {
	storepb.StoreServer
	newSeriesLimiter      SeriesLimiterFactory
	newSamplesLimiter     ChunksLimiterFactory
	failedRequestsCounter *prometheus.CounterVec
}

// NewLimitedStoreServer creates a new limitedStoreServer.
func NewLimitedStoreServer(store storepb.StoreServer, reg prometheus.Registerer, selectLimits SeriesSelectLimits) storepb.StoreServer {
	return &limitedStoreServer{
		StoreServer:       store,
		newSeriesLimiter:  NewSeriesLimiterFactory(selectLimits.SeriesPerRequest),
		newSamplesLimiter: NewChunksLimiterFactory(selectLimits.SamplesPerRequest),
		failedRequestsCounter: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_selects_dropped_total",
			Help: "Number of select queries that were dropped due to configured limits.",
		}, []string{"reason"}),
	}
}

func (s *limitedStoreServer) Series(req *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	seriesLimiter := s.newSeriesLimiter(s.failedRequestsCounter.WithLabelValues("series"))
	chunksLimiter := s.newSamplesLimiter(s.failedRequestsCounter.WithLabelValues("chunks"))
	limitedSrv := newLimitedServer(srv, seriesLimiter, chunksLimiter)
	if err := s.StoreServer.Series(req, limitedSrv); err != nil {
		return err
	}

	return nil
}

var _ storepb.Store_SeriesServer = &limitedServer{}

// limitedServer is a storepb.Store_SeriesServer that tracks statistics about sent series.
type limitedServer struct {
	storepb.Store_SeriesServer
	seriesLimiter  SeriesLimiter
	samplesLimiter ChunksLimiter
}

func newLimitedServer(upstream storepb.Store_SeriesServer, seriesLimiter SeriesLimiter, chunksLimiter ChunksLimiter) *limitedServer {
	return &limitedServer{
		Store_SeriesServer: upstream,
		seriesLimiter:      seriesLimiter,
		samplesLimiter:     chunksLimiter,
	}
}

func (i *limitedServer) Send(response *storepb.SeriesResponse) error {
	series := response.GetSeries()
	if series == nil {
		return i.Store_SeriesServer.Send(response)
	}

	// Print labels of each series
	fmt.Printf("Labels: %v\n", series.Labels)

	// Calculate min and max time of the chunks
	var minTime, maxTime int64
	if len(series.Chunks) > 0 {
		minTime = series.Chunks[0].MinTime
		maxTime = series.Chunks[0].MaxTime
		for _, chunk := range series.Chunks {
			if chunk.MinTime < minTime {
				minTime = chunk.MinTime
			}
			if chunk.MaxTime > maxTime {
				maxTime = chunk.MaxTime
			}
		}
	}
	fmt.Printf("Min Time: %s, Max Time: %s\n", time.Unix(minTime, 0).Format(time.RFC3339), time.Unix(maxTime, 0).Format(time.RFC3339))

	// Print hash of the chunks
	for _, chunk := range series.Chunks {
		hash := sha256.Sum256(chunk.Raw.Data)
		fmt.Printf("Chunk Hash: %s\n", hex.EncodeToString(hash[:]))
	}

	if err := i.seriesLimiter.Reserve(1); err != nil {
		return errors.Wrapf(err, "failed to send series")
	}
	//fmt.Printf("Checking chunks with IDs: %v\n", series.Chunks)
	if err := i.samplesLimiter.Reserve(uint64(len(series.Chunks) * MaxSamplesPerChunk)); err != nil {
		return errors.Wrapf(err, "failed to send samples")
	}

	return i.Store_SeriesServer.Send(response)
}
