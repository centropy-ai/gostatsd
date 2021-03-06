package main

import (
	"context"
	_ "expvar"
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/atlassian/gostatsd/pkg/util"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/backends"
	"github.com/atlassian/gostatsd/pkg/statsd"
	"github.com/atlassian/gostatsd/pkg/transport"

	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"golang.org/x/time/rate"
)

const (
	// ParamVerbose enables verbose logging.
	ParamVerbose = "verbose"
	// ParamProfile enables profiler endpoint on the specified address and port.
	ParamProfile = "profile"
	// ParamJSON makes logger log in JSON format.
	ParamJSON = "json"
	// ParamConfigPath provides file with configuration.
	ParamConfigPath = "config-path"
	// ParamVersion makes program output its version.
	ParamVersion = "version"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	v, version, err := setupConfiguration()
	if err != nil {
		if err == pflag.ErrHelp {
			return
		}
		logrus.Fatalf("Error while parsing configuration: %v", err)
	}
	if version {
		fmt.Printf("Version: %s - Commit: %s - Date: %s\n", Version, GitCommit, BuildDate)
		return
	}
	if err := run(v); err != nil {
		logrus.Fatalf("%v", err)
	}
}

func run(v *viper.Viper) error {
	profileAddr := v.GetString(ParamProfile)
	if profileAddr != "" {
		go func() {
			logrus.Errorf("Profiler server failed: %v", http.ListenAndServe(profileAddr, nil))
		}()
	}

	logrus.Info("Starting server")
	s, err := constructServer(v)
	if err != nil {
		return err
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	cancelOnInterrupt(ctx, cancelFunc)

	if err := s.Run(ctx); err != nil && err != context.Canceled {
		return fmt.Errorf("server error: %v", err)
	}
	return nil
}

func constructServer(v *viper.Viper) (*statsd.Server, error) {
	// Logger
	logger := logrus.StandardLogger()

	// HTTP client pool
	pool := transport.NewTransportPool(logger, v)

	// Cloud handler factory
	cloud, err := statsd.NewCloudHandlerFactoryFromViper(v, logger, Version)
	if err != nil {
		return nil, err
	}
	if cloud != nil {
		if err := cloud.InitCloudProvider(v); err != nil {
			return nil, err
		}
	}
	// Backends
	backendNames := v.GetStringSlice(statsd.ParamBackends)
	backendsList := make([]gostatsd.Backend, len(backendNames))
	for i, backendName := range backendNames {
		backend, errBackend := backends.InitBackend(backendName, v, pool)
		if errBackend != nil {
			return nil, errBackend
		}
		backendsList[i] = backend
	}
	// Percentiles
	pt, err := getPercentiles(v.GetStringSlice(statsd.ParamPercentThreshold))
	if err != nil {
		return nil, err
	}
	// Create server
	return &statsd.Server{
		Backends:            backendsList,
		CloudHandlerFactory: cloud,
		InternalTags:        v.GetStringSlice(statsd.ParamInternalTags),
		InternalNamespace:   v.GetString(statsd.ParamInternalNamespace),
		DefaultTags:         v.GetStringSlice(statsd.ParamDefaultTags),
		Hostname:            v.GetString(statsd.ParamHostname),
		ExpiryInterval:      v.GetDuration(statsd.ParamExpiryInterval),
		FlushInterval:       v.GetDuration(statsd.ParamFlushInterval),
		IgnoreHost:          v.GetBool(statsd.ParamIgnoreHost),
		MaxReaders:          v.GetInt(statsd.ParamMaxReaders),
		MaxParsers:          v.GetInt(statsd.ParamMaxParsers),
		MaxWorkers:          v.GetInt(statsd.ParamMaxWorkers),
		MaxQueueSize:        v.GetInt(statsd.ParamMaxQueueSize),
		MaxConcurrentEvents: v.GetInt(statsd.ParamMaxConcurrentEvents),
		EstimatedTags:       v.GetInt(statsd.ParamEstimatedTags),
		MetricsAddr:         v.GetString(statsd.ParamMetricsAddr),
		Namespace:           v.GetString(statsd.ParamNamespace),
		StatserType:         v.GetString(statsd.ParamStatserType),
		PercentThreshold:    pt,
		HeartbeatEnabled:    v.GetBool(statsd.ParamHeartbeatEnabled),
		ReceiveBatchSize:    v.GetInt(statsd.ParamReceiveBatchSize),
		ConnPerReader:       v.GetBool(statsd.ParamConnPerReader),
		ServerMode:          v.GetString(statsd.ParamServerMode),
		LogRawMetric:        v.GetBool(statsd.ParamLogRawMetric),
		HeartbeatTags: gostatsd.Tags{
			fmt.Sprintf("version:%s", Version),
			fmt.Sprintf("commit:%s", GitCommit),
		},
		DisabledSubTypes:          gostatsd.DisabledSubMetrics(v),
		BadLineRateLimitPerSecond: rate.Limit(v.GetFloat64(statsd.ParamBadLinesPerMinute) / 60.0),
		Viper:                     v,
		TransportPool:             pool,
	}, nil
}

func getPercentiles(s []string) ([]float64, error) {
	percentThresholds := make([]float64, len(s))
	for i, sPercentThreshold := range s {
		pt, err := strconv.ParseFloat(sPercentThreshold, 64)
		if err != nil {
			return nil, err
		}
		percentThresholds[i] = pt
	}
	return percentThresholds, nil
}

// cancelOnInterrupt calls f when os.Interrupt or SIGTERM is received.
func cancelOnInterrupt(ctx context.Context, f context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case <-ctx.Done():
		case <-c:
			f()
		}
	}()
}

func setupConfiguration() (*viper.Viper, bool, error) {
	v := viper.New()
	defer setupLogger(v) // Apply logging configuration in case of early exit
	util.InitViper(v, "")

	var version bool

	cmd := pflag.NewFlagSet(os.Args[0], pflag.ContinueOnError)

	cmd.BoolVar(&version, ParamVersion, false, "Print the version and exit")
	cmd.Bool(ParamVerbose, false, "Verbose")
	cmd.Bool(ParamJSON, false, "Log in JSON format")
	cmd.String(ParamProfile, "", "Enable profiler endpoint on the specified address and port")
	cmd.String(ParamConfigPath, "", "Path to the configuration file")

	statsd.AddFlags(cmd)

	cmd.VisitAll(func(flag *pflag.Flag) {
		if err := v.BindPFlag(flag.Name, flag); err != nil {
			panic(err) // Should never happen
		}
	})

	if err := cmd.Parse(os.Args[1:]); err != nil {
		return nil, false, err
	}

	configPath := v.GetString(ParamConfigPath)
	if configPath != "" {
		v.SetConfigFile(configPath)
		if err := v.ReadInConfig(); err != nil {
			return nil, false, err
		}
	}

	return v, version, nil
}

func setupLogger(v *viper.Viper) {
	if v.GetBool(ParamVerbose) {
		logrus.SetLevel(logrus.DebugLevel)
	}
	if v.GetBool(ParamJSON) {
		logrus.SetFormatter(&logrus.JSONFormatter{})
	}
}
