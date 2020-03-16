package cloudprovider

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ash2k/stager/wait"
	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/stats"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

func NewCachedCloudProvider(logger logrus.FieldLogger, limiter *rate.Limiter, cloudProvider gostatsd.CloudProvider, cacheOpts gostatsd.CacheOptions) *CachedCloudProvider {
	return &CachedCloudProvider{
		logger:         logger,
		limiter:        limiter,
		cloudProvider:  cloudProvider,
		cacheOpts:      cacheOpts,
		ipSinkSource:   make(chan gostatsd.IP),
		infoSinkSource: make(chan gostatsd.InstanceInfo),
		emitChan:       make(chan stats.Statser),
		cache:          make(map[gostatsd.IP]*instanceHolder),
	}
}

type CachedCloudProvider struct {
	// These fields may only be read or written by the main CachedCloudProvider.Run goroutine
	statsCacheRefreshPositive uint64 // Cumulative number of positive refreshes (ie, a refresh which succeeded)
	statsCacheRefreshNegative uint64 // Cumulative number of negative refreshes (ie, a refresh which failed and used old data)
	statsCachePositive        uint64 // Absolute number of positive entries in cache
	statsCacheNegative        uint64 // Absolute number of negative entries in cache

	logger         logrus.FieldLogger
	limiter        *rate.Limiter
	cloudProvider  gostatsd.CloudProvider
	cacheOpts      gostatsd.CacheOptions
	ipSinkSource   chan gostatsd.IP
	infoSinkSource chan gostatsd.InstanceInfo

	// emitChan triggers a write of all the current stats when it is given a Statser
	emitChan     chan stats.Statser
	rw           sync.RWMutex // Protects cache
	cache        map[gostatsd.IP]*instanceHolder
	toLookupIPs  []gostatsd.IP
	toReturnInfo []gostatsd.InstanceInfo
}

func (ch *CachedCloudProvider) Run(ctx context.Context) {
	var (
		toLookupC     chan<- gostatsd.IP
		toLookupIP    gostatsd.IP
		toReturnInfoC chan<- gostatsd.InstanceInfo
		toReturnInfo  gostatsd.InstanceInfo
		wg            wait.Group
	)
	// this goroutine needs to populate/update the cache so an intermediate InstanceInfo channel is used below that allows
	// to intercept, update the cache and then push the information through to the cache consumer.
	ownInfoSource := make(chan gostatsd.InstanceInfo)
	ld := cloudProviderLookupDispatcher{
		logger:        ch.logger,
		limiter:       ch.limiter,
		cloudProvider: ch.cloudProvider,
		ipSource:      ch.ipSinkSource, // our sink is their source
		infoSink:      ownInfoSource,   // their sink is our source
	}

	defer wg.Wait() // Wait for cloudProviderLookupDispatcher to stop

	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // Tell CachedCloudProvider to stop

	wg.StartWithContext(ctx, ld.run)

	refreshTicker := time.NewTicker(ch.cacheOpts.CacheRefreshPeriod)
	defer refreshTicker.Stop()
	// No locking for ch.cache READ access required - this goroutine owns the object and only it mutates it.
	// So reads from the same goroutine are always safe (no concurrent mutations).
	// When we mutate the cache, we hold the exclusive (write) lock to avoid concurrent reads.
	// When we read from the cache from other goroutines in the Peek() method, we obtain the read lock.
	for {
		select {
		case <-ctx.Done():
			return
		case toLookupC <- toLookupIP:
			toLookupIP = gostatsd.UnknownIP // enable GC
			toLookupC = nil                 // ip has been sent; if there is nothing to send, the case is disabled
		case toReturnInfoC <- toReturnInfo:
			toReturnInfo = gostatsd.InstanceInfo{} // enable GC
			toReturnInfoC = nil                    // info has been sent; if there is nothing to send, the case is disabled
		case info := <-ownInfoSource:
			ch.handleInstanceInfo(info)
		case t := <-refreshTicker.C:
			ch.doRefresh(t)
		case statser := <-ch.emitChan:
			ch.emit(statser)
		}
		if toLookupC == nil && len(ch.toLookupIPs) > 0 {
			last := len(ch.toLookupIPs) - 1
			toLookupIP = ch.toLookupIPs[last]
			ch.toLookupIPs[last] = gostatsd.UnknownIP // enable GC
			ch.toLookupIPs = ch.toLookupIPs[:last]
			toLookupC = ch.ipSinkSource
		}
		if toReturnInfoC == nil && len(ch.toReturnInfo) > 0 {
			last := len(ch.toReturnInfo) - 1
			toReturnInfo = ch.toReturnInfo[last]
			ch.toReturnInfo[last] = gostatsd.InstanceInfo{} // enable GC
			ch.toReturnInfo = ch.toReturnInfo[:last]
			toReturnInfoC = ch.infoSinkSource
		}
	}
}

func (ch *CachedCloudProvider) Peek(ip gostatsd.IP) (*gostatsd.Instance, bool /*is a cache hit*/) {
	ch.rw.RLock()
	holder, existsInCache := ch.cache[ip]
	ch.rw.RUnlock()
	if !existsInCache {
		return nil, false
	}
	holder.updateAccess()
	return holder.instance, true // can be nil, true
}

func (ch *CachedCloudProvider) IpSink() chan<- gostatsd.IP {
	return ch.ipSinkSource
}

func (ch *CachedCloudProvider) InfoSource() <-chan gostatsd.InstanceInfo {
	return ch.infoSinkSource
}

func (ch *CachedCloudProvider) EstimatedTags() int {
	return ch.cloudProvider.EstimatedTags()
}

func (ch *CachedCloudProvider) RunMetrics(ctx context.Context, statser stats.Statser) {
	// All the channels are unbuffered, so no CSWs
	flushed, unregister := statser.RegisterFlush()
	defer unregister()

	for {
		select {
		case <-ctx.Done():
			return
		case <-flushed:
			ch.scheduleEmit(ctx, statser)
		}
	}
}

// scheduleEmit is used to push a request to the main goroutine requesting metrics
// be emitted.  This is done so we can skip atomic operations on most of our metric
// counters.  In line with the flush notifier, it is fire and forget and won't block
func (ch *CachedCloudProvider) scheduleEmit(ctx context.Context, statser stats.Statser) {
	select {
	case ch.emitChan <- statser:
		// success
	case <-ctx.Done():
		// success-ish
	default:
		// at least we tried
	}
}

func (ch *CachedCloudProvider) emit(statser stats.Statser) {
	// regular
	statser.Gauge("cloudprovider.cache_positive", float64(ch.statsCachePositive), nil)
	statser.Gauge("cloudprovider.cache_negative", float64(ch.statsCacheNegative), nil)
	statser.Gauge("cloudprovider.cache_refresh_positive", float64(ch.statsCacheRefreshPositive), nil)
	statser.Gauge("cloudprovider.cache_refresh_negative", float64(ch.statsCacheRefreshNegative), nil)
}

func (ch *CachedCloudProvider) doRefresh(t time.Time) {
	var toDelete []gostatsd.IP
	now := t.UnixNano()
	idleNano := ch.cacheOpts.CacheEvictAfterIdlePeriod.Nanoseconds()

	for ip, holder := range ch.cache {
		if now-holder.lastAccess() > idleNano {
			// Entry was not used recently, remove it.
			toDelete = append(toDelete, ip)
			if holder.instance == nil {
				ch.statsCacheNegative--
			} else {
				ch.statsCachePositive--
			}
		} else if t.After(holder.expires) {
			// Entry needs a refresh.
			ch.toLookupIPs = append(ch.toLookupIPs, ip)
		}
	}

	if len(toDelete) > 0 {
		ch.rw.Lock()
		defer ch.rw.Unlock()
		for _, ip := range toDelete {
			delete(ch.cache, ip)
		}
	}
}

func (ch *CachedCloudProvider) handleInstanceInfo(info gostatsd.InstanceInfo) {
	var ttl time.Duration
	if info.Instance == nil {
		ttl = ch.cacheOpts.CacheNegativeTTL
	} else {
		ttl = ch.cacheOpts.CacheTTL
	}
	now := time.Now()
	newHolder := &instanceHolder{
		expires:  now.Add(ttl),
		instance: info.Instance,
	}
	currentHolder := ch.cache[info.IP]
	if currentHolder == nil {
		// Not in cache, count it
		if info.Instance == nil {
			ch.statsCacheNegative++
		} else {
			ch.statsCachePositive++
		}
		newHolder.lastAccessNano = now.UnixNano()
	} else {
		// In cache, don't count it
		newHolder.lastAccessNano = currentHolder.lastAccess()
		if info.Instance == nil {
			// Use the old instance if there was a lookup error.
			newHolder.instance = currentHolder.instance
			ch.statsCacheRefreshNegative++
		} else {
			if currentHolder.instance == nil && newHolder.instance != nil {
				// An entry has flipped from invalid to valid
				ch.statsCacheNegative--
				ch.statsCachePositive++
			}
			ch.statsCacheRefreshPositive++
		}
	}
	ch.rw.Lock()
	ch.cache[info.IP] = newHolder
	ch.rw.Unlock()
	ch.toReturnInfo = append(ch.toReturnInfo, info)
}

type instanceHolder struct {
	lastAccessNano int64
	expires        time.Time          // When this record expires.
	instance       *gostatsd.Instance // Can be nil if the lookup resulted in an error or instance was not found
}

func (ih *instanceHolder) updateAccess() {
	atomic.StoreInt64(&ih.lastAccessNano, time.Now().UnixNano())
}

func (ih *instanceHolder) lastAccess() int64 {
	return atomic.LoadInt64(&ih.lastAccessNano)
}
