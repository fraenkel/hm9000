package hm

import (
	"os"
	"strconv"

	"github.com/cloudfoundry/hm9000/config"
	"github.com/cloudfoundry/hm9000/desiredstatefetcher"
	"github.com/cloudfoundry/hm9000/helpers/httpclient"
	"github.com/cloudfoundry/hm9000/helpers/logger"
	"github.com/cloudfoundry/hm9000/helpers/metricsaccountant"
	"github.com/cloudfoundry/hm9000/store"
)

func FetchDesiredState(l logger.Logger, conf *config.Config, poll bool) {
	store, _ := connectToStore(l, conf)

	if poll {
		l.Info("Starting Desired State Daemon...")

		adapter, _ := connectToStoreAdapter(l, conf)

		err := Daemonize("Fetcher", func() error {
			return fetchDesiredState(l, conf, store)
		}, conf.FetcherPollingInterval(), conf.FetcherTimeout(), l, adapter)
		if err != nil {
			l.Error("Desired State Daemon Errored", err)
		}
		l.Info("Desired State Daemon is Down")
		os.Exit(1)
	} else {
		err := fetchDesiredState(l, conf, store)
		if err != nil {
			os.Exit(1)
		} else {
			os.Exit(0)
		}
	}
}

func FetchDBDesiredState(l logger.Logger, conf *config.Config, poll bool) {
	store, _ := connectToStore(l, conf)

	if poll {
		l.Info("Starting Desired State Daemon...")

		adapter, _ := connectToStoreAdapter(l, conf)

		err := Daemonize("Fetcher", func() error {
			return fetchDBDesiredState(l, conf, store)
		}, conf.FetcherPollingInterval(), conf.FetcherTimeout(), l, adapter)
		if err != nil {
			l.Error("Desired State Daemon Errored", err)
		}
		l.Info("Desired State Daemon is Down")
		os.Exit(1)
	} else {
		err := fetchDesiredState(l, conf, store)
		if err != nil {
			os.Exit(1)
		} else {
			os.Exit(0)
		}
	}
}

func fetchDesiredState(l logger.Logger, conf *config.Config, store store.Store) error {
	l.Info("Fetching Desired State")
	fetcher := desiredstatefetcher.New(conf,
		store,
		metricsaccountant.New(store),
		httpclient.NewHttpClient(conf.SkipSSLVerification, conf.FetcherNetworkTimeout()),
		buildTimeProvider(l),
		l,
	)

	resultChan := make(chan desiredstatefetcher.DesiredStateFetcherResult, 1)
	fetcher.Fetch(resultChan)

	result := <-resultChan

	if result.Success {
		l.Info("Success", map[string]string{"Number of Desired Apps Fetched": strconv.Itoa(result.NumResults)})
		return nil
	} else {
		l.Error(result.Message, result.Error)
		return result.Error
	}
	return nil
}

func fetchDBDesiredState(l logger.Logger, conf *config.Config, store store.Store) error {
	l.Info("Fetching Desired State")
	fetcher := desiredstatefetcher.NewDBFetcher(conf,
		store,
		metricsaccountant.New(store),
		buildTimeProvider(l),
		l,
	)

	resultChan := make(chan desiredstatefetcher.DesiredStateFetcherResult, 1)
	fetcher.Fetch(resultChan)

	result := <-resultChan

	if result.Success {
		l.Info("Success", map[string]string{"Number of Desired Apps Fetched": strconv.Itoa(result.NumResults)})
		return nil
	} else {
		l.Error(result.Message, result.Error)
		return result.Error
	}
	return nil
}
