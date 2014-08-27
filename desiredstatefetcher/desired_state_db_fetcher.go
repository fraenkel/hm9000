package desiredstatefetcher

import (
	"database/sql"
	"strconv"
	"strings"
	"time"

	"github.com/cloudfoundry/gunk/timeprovider"
	"github.com/cloudfoundry/hm9000/config"
	"github.com/cloudfoundry/hm9000/helpers/logger"
	"github.com/cloudfoundry/hm9000/helpers/metricsaccountant"
	"github.com/cloudfoundry/hm9000/models"
	"github.com/cloudfoundry/hm9000/store"
	_ "github.com/lib/pq"
)

type DesiredStateDBFetcher struct {
	config            *config.Config
	db                *sql.DB
	store             store.Store
	metricsAccountant metricsaccountant.MetricsAccountant
	timeProvider      timeprovider.TimeProvider
	cache             map[string]models.DesiredAppState
	logger            logger.Logger
}

func NewDBFetcher(config *config.Config,
	store store.Store,
	metricsAccountant metricsaccountant.MetricsAccountant,
	timeProvider timeprovider.TimeProvider,
	logger logger.Logger) *DesiredStateDBFetcher {

	return &DesiredStateDBFetcher{
		config:            config,
		store:             store,
		metricsAccountant: metricsAccountant,
		timeProvider:      timeProvider,
		cache:             map[string]models.DesiredAppState{},
		logger:            logger,
	}
}

func (fetcher *DesiredStateDBFetcher) Fetch(resultChan chan DesiredStateFetcherResult) {
	if fetcher.db == nil {
		db, err := sql.Open("postgres", "postgres://ccadmin:admin@10.244.0.30:5524/ccdb?sslmode=disable")
		if err != nil {
			resultChan <- DesiredStateFetcherResult{Message: "Failed to Open the DB", Error: err}
			return
		}
		fetcher.db = db
	}

	rows, err := fetcher.db.Query("SELECT guid, version, instances, state, package_state FROM apps WHERE deleted_at is NULL AND diego = false ORDER BY id")
	if err != nil {
		resultChan <- DesiredStateFetcherResult{Message: "Failed to query desired state", Error: err}
		return
	}
	defer rows.Close()

	numRows, err := fetcher.cacheRows(rows)
	if err != nil {
		resultChan <- DesiredStateFetcherResult{Message: "Failed to cache desired state", Error: err}
		return
	}

	tSync := time.Now()
	err = fetcher.syncStore()
	fetcher.metricsAccountant.TrackDesiredStateSyncTime(time.Since(tSync))
	if err != nil {
		resultChan <- DesiredStateFetcherResult{Message: "Failed to sync desired state to the store", Error: err}
		return
	}

	fetcher.store.BumpDesiredFreshness(fetcher.timeProvider.Time())
	resultChan <- DesiredStateFetcherResult{Success: true, NumResults: numRows}
}

func (fetcher *DesiredStateDBFetcher) guids(desiredStates []models.DesiredAppState) string {
	result := make([]string, len(desiredStates))

	for i, desired := range desiredStates {
		result[i] = desired.AppGuid
	}

	return strings.Join(result, ",")
}

func (fetcher *DesiredStateDBFetcher) syncStore() error {
	desiredStates := make([]models.DesiredAppState, len(fetcher.cache))
	i := 0
	for _, desiredState := range fetcher.cache {
		desiredStates[i] = desiredState
		i++
	}
	err := fetcher.store.SyncDesiredState(desiredStates...)
	if err != nil {
		fetcher.logger.Error("Failed to Sync Desired State", err, map[string]string{
			"Number of Entries": strconv.Itoa(len(desiredStates)),
			"Desireds":          fetcher.guids(desiredStates),
		})
		return err
	}

	return nil
}

func (fetcher *DesiredStateDBFetcher) cacheRows(rows *sql.Rows) (int, error) {
	cnt := 0
	for rows.Next() {
		var guid string
		var version string
		var instances int
		var state string
		var pState string
		if err := rows.Scan(&guid, &version, &instances, &state, &pState); err != nil {
			return 0, err
		}

		cnt++
		appState := models.AppState(state)
		packageState := models.AppPackageState(pState)

		if appState == models.AppStateStarted && (packageState == models.AppPackageStateStaged || packageState == models.AppPackageStatePending) {
			desiredState := models.DesiredAppState{
				AppGuid:           guid,
				AppVersion:        version,
				NumberOfInstances: instances,
				State:             appState,
				PackageState:      packageState,
			}
			fetcher.cache[desiredState.StoreKey()] = desiredState
		}
	}

	err := rows.Err()
	return cnt, err
}
