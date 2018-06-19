package main

import (
	"fmt"
	"os"
	"time"

	// external
	pp "github.com/k0kubun/pp"
	storage "github.com/sniperkit/colly-storage/pkg"
	store_pivot "github.com/sniperkit/colly-storage/plugin/dal/pivot"

	// internal
	queue "github.com/sniperkit/colly-storage/queue"
)

var (
	storageDebug        bool          = false
	storagePingDuration time.Duration = 5 * time.Second
)

var queue *queue.Queue

func main() {

	fmt.Println("Running storage `colly-pivot-sqlite3-queue` example...")

	storageConfig := &store_pivot.Config{
		Scheme:  "sqlite",                                 // required
		Host:    "",                                       // required
		Dataset: "./shared/storage/sqlite/colly-queue.db", // required
		Options: map[string]interface{}{},                 // optional
	}

	store, err := store_pivot.NewDataAbstractionLayer(storageConfig)
	if err != nil {
		fmt.Println("error while creating a new data abstraction layer instance... error=", err)
		os.Exit(1)
	}

	if storageDebug {
		pp.Println("Storage=", store)
	} else {
		store.Action("ping", storagePingDuration)
		store.Action("list_collections", nil)
	}

}

/*
func createCollection(name string) error {
	return store.CreateCollection(
		store.NewCollection(name),
	)
}

func checkCollection(name string) error {
	if coll, err := store.GetCollection(name); err != nil {
		return err
	}
	return nil
}

func loadData(collection string) error {

	// Create collection with schema
	// --------------------------------------------------------------------------------------------
	err := store.CreateCollection(
		store.NewCollection(collection).
			AddFields(store.Field{
				Name: `name`,
				Type: store.StringType,
			}, store.Field{
				Name:         `created_at`,
				Type:         store.TimeType,
				DefaultValue: time.Now,
			}))

	var record *store.Record

	// Insert and Retrieve
	// --------------------------------------------------------------------------------------------
	recordset := store.NewRecordSet(
		store.NewRecord(testCrudIdSet[0]).Set(`name`, `First`),
		store.NewRecord(testCrudIdSet[1]).Set(`name`, `Second`),
		store.NewRecord(testCrudIdSet[2]).Set(`name`, `Third`))
}

func search(collection string) error {
	collection := store.NewCollection(`TestSearchQuery`).
		AddFields(store.Field{
			Name: `name`,
			Type: store.StringType,
		})

	// set the global page size at the package level for this test
	backends.IndexerPageSize = 5
	backends.IndexerPageSize = 100
	if search := store.WithSearch(collection); search != nil {
		err := store.CreateCollection(collection)

		// f, err := filter.Parse(`all`)
		// f.Offset = 20
		// f.Limit = 9
		// keyValues, err := search.ListValues(collection, []string{`name`}, filter.All())
		// v, ok := keyValues[`name`]
	}
	// if agg := backend.WithAggregator(collection); agg != nil {
	// }
}
*/
