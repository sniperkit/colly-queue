package main

import (
	"fmt"
	"os"

	// colly - core
	colly "github.com/sniperkit/colly/pkg"

	// colly - internal
	queue "github.com/sniperkit/colly-queue/pkg"

	// colly - plugin
	storage "github.com/sniperkit/colly-storage/pkg"
	store_pivot "github.com/sniperkit/colly-storage/plugin/dal/pivot"
)

func main() {

	fmt.Println("Running storage `colly-pivot-sqlite3-queue` example...")

	url := "https://httpbin.org/delay/1"

	// Instantiate default collector
	c := colly.NewCollector(colly.AllowURLRevisit())

	// create a request queue with 2 consumer threads
	q, _ := queue.New(
		2, // Number of consumer threads
		initStorageByDefault(), // Use default queue storage
	)

	c.OnRequest(func(r *colly.Request) {
		fmt.Println("visiting", r.URL)
		if r.ID < 15 {
			r2, err := r.New("GET", fmt.Sprintf("%s?x=%v", url, r.ID), nil)
			if err == nil {
				q.AddRequest(r2)
			}
		}
	})

	for i := 0; i < 5; i++ {
		// Add URLs to the queue
		q.AddURL(fmt.Sprintf("%s?n=%d", url, i))
	}
	// Consume URLs
	q.Run(c)

}

func initStorageByDefault() storage.Storage {
	store, err := store_pivot.NewDataAbstractionLayer(
		&store_pivot.Config{
			Scheme:  "sqlite",                                 // required
			Host:    "",                                       // required
			Dataset: "./shared/storage/sqlite/colly-queue.db", // required
			Options: map[string]interface{}{},                 // optional
		},
	)
	if err != nil {
		fmt.Println("error while creating a new data abstraction layer instance... error=", err)
		os.Exit(1)
	}
	return store
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
