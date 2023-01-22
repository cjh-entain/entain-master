package db

import (
	"database/sql"
	"testing"
	"time"

	"git.neds.sh/matty/entain/racing/proto/racing"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	testTableDefinition = `CREATE TABLE IF NOT EXISTS races (id INTEGER PRIMARY KEY, meeting_id INTEGER, name TEXT, number INTEGER, visible INTEGER, advertised_start_time DATETIME)`
)

type applyFilterConfig struct {
	Filter        *racing.ListRacesRequestFilter
	ExpectedQuery string
	ExpectedArgs  []interface{}
}

// Validates .applyFilter by comparing query strings. As the modification occurs at the end of the query string, these
// unit tests are validating only the end of the query. This avoids an additional unnecessary dependency on
// getQueryStrings().
func Test_RacesRepo_applyFilter(t *testing.T) {

	tests := map[string]applyFilterConfig{
		"Base Case - No filters": {
			Filter:        &racing.ListRacesRequestFilter{},
			ExpectedQuery: "",
		},
		"Filter on single MeetingId": {
			Filter: &racing.ListRacesRequestFilter{
				MeetingIds: []int64{1},
			},
			ExpectedArgs:  []interface{}{int64(1)},
			ExpectedQuery: " WHERE meeting_id IN (?)",
		},
		"Filter on multiple MeetingId's": {
			Filter: &racing.ListRacesRequestFilter{
				MeetingIds: []int64{1, 2},
			},
			ExpectedArgs:  []interface{}{int64(1), int64(2)},
			ExpectedQuery: " WHERE meeting_id IN (?,?)",
		},
		"Filter on Visible = true": {
			Filter: &racing.ListRacesRequestFilter{
				Visible: pointerTo(true),
			},
			ExpectedQuery: " WHERE visible = true",
		},
		"Filter on Visible = false": {
			Filter: &racing.ListRacesRequestFilter{
				Visible: pointerTo(false),
			},
			ExpectedQuery: " WHERE visible = false",
		},
		"Filter on both MeetingId's and Visible": {
			Filter: &racing.ListRacesRequestFilter{
				MeetingIds: []int64{1, 2},
				Visible:    pointerTo(true),
			},
			ExpectedArgs:  []interface{}{int64(1), int64(2)},
			ExpectedQuery: " WHERE meeting_id IN (?,?) AND visible = true",
		},
		"Filter on a Race ID": {
			Filter: &racing.ListRacesRequestFilter{
				Id: pointerTo(int64(5)),
			},
			ExpectedArgs:  []interface{}{int64(5)},
			ExpectedQuery: " WHERE id = ?",
		},
	}

	// Create DB & RacesRepo struct
	racesDB := memoryDB(t)
	defer racesDB.Close()
	racesRepo := &racesRepo{
		db: racesDB,
	}

	// Run tests
	for name, cfg := range tests {
		t.Run(
			name,
			func(cfg applyFilterConfig) func(t *testing.T) {
				return func(*testing.T) {
					gotQuery, gotArgs := racesRepo.applyFilter("", cfg.Filter)

					assert.Equal(t, cfg.ExpectedArgs, gotArgs)
					assert.Equal(t, cfg.ExpectedQuery, gotQuery)
				}
			}(cfg))
	}
}

type applyOrderConfig struct {
	Order         *racing.ListRacesRequestOrder
	ExpectedQuery string
}

// Validates .applyOrder by comparing query strings. As with .applyFilter, these unit tests are validating only the end
// of the query string and any associated arguments.
func Test_RacesRepo_applyOrder(t *testing.T) {

	tests := map[string]applyOrderConfig{
		"Base case - No order provided": {
			Order:         nil,
			ExpectedQuery: "",
		},
		"Order with no field and no direction": {
			Order: &racing.ListRacesRequestOrder{
				Field:     nil,
				Direction: nil,
			},
			ExpectedQuery: " ORDER BY advertised_start_time",
		},
		"Order with no field but direction included": {
			Order: &racing.ListRacesRequestOrder{
				Field:     nil,
				Direction: pointerTo("ASC"),
			},
			ExpectedQuery: " ORDER BY advertised_start_time ASC",
		},
		"Order with no field and invalid direction": {
			Order: &racing.ListRacesRequestOrder{
				Field:     nil,
				Direction: pointerTo("INVALID"),
			},
			ExpectedQuery: " ORDER BY advertised_start_time",
		},
		"Order provided for invalid field, no direction": {
			Order: &racing.ListRacesRequestOrder{
				Field:     pointerTo("unknown"),
				Direction: nil,
			},
			ExpectedQuery: "",
		},
		"Order provided for invalid field with direction resulting in no changes": {
			Order: &racing.ListRacesRequestOrder{
				Field:     pointerTo("unknown"),
				Direction: pointerTo("ASC"),
			},
			ExpectedQuery: "",
		},
		"Order provided for valid field, no direction": {
			Order: &racing.ListRacesRequestOrder{
				Field:     pointerTo("meeting_id"),
				Direction: nil,
			},
			ExpectedQuery: " ORDER BY meeting_id",
		},
		"Order provided for valid field, ASC direction": {
			Order: &racing.ListRacesRequestOrder{
				Field:     pointerTo("meeting_id"),
				Direction: pointerTo("ASC"),
			},
			ExpectedQuery: " ORDER BY meeting_id ASC",
		},
		"Order provided for valid field, DESC direction": {
			Order: &racing.ListRacesRequestOrder{
				Field:     pointerTo("meeting_id"),
				Direction: pointerTo("DESC"),
			},
			ExpectedQuery: " ORDER BY meeting_id DESC",
		},
		"Order provided for valid field, invalid direction": {
			Order: &racing.ListRacesRequestOrder{
				Field:     pointerTo("meeting_id"),
				Direction: pointerTo("INCORRECT"),
			},
			ExpectedQuery: " ORDER BY meeting_id",
		},
	}

	// Create DB & RacesRepo struct
	racesDB := memoryDB(t)
	defer racesDB.Close()
	racesRepo := &racesRepo{
		db: racesDB,
	}

	// Run tests
	for name, cfg := range tests {
		t.Run(
			name,
			func(cfg applyOrderConfig) func(t *testing.T) {
				return func(*testing.T) {
					gotQuery := racesRepo.applyOrder("", cfg.Order)
					assert.Equal(t, cfg.ExpectedQuery, gotQuery)
				}
			}(cfg))
	}
}

type addStatusConfig struct {
	Input         []*racing.Race
	ExpectedRaces []*racing.Race
}

// Validates the calculation of the status field based upon a races timestamp
func Test_addStatus(t *testing.T) {

	var (
		futureTime = timestamppb.New(time.Now().Add(time.Hour * 24))
		pastTime   = timestamppb.New(time.Now().Add(-time.Hour * 24))
	)

	tests := map[string]addStatusConfig{
		"No input": {
			Input:         nil,
			ExpectedRaces: nil,
		},
		"Single race with future time": {
			Input: []*racing.Race{
				{AdvertisedStartTime: futureTime},
			},
			ExpectedRaces: []*racing.Race{
				{AdvertisedStartTime: futureTime, Status: "OPEN"},
			},
		},
		"Single race with past time": {
			Input: []*racing.Race{
				{AdvertisedStartTime: pastTime},
			},
			ExpectedRaces: []*racing.Race{
				{AdvertisedStartTime: pastTime, Status: "CLOSED"},
			},
		},
		"Multiple races with future times": {
			Input: []*racing.Race{
				{AdvertisedStartTime: futureTime},
				{AdvertisedStartTime: futureTime},
			},
			ExpectedRaces: []*racing.Race{
				{AdvertisedStartTime: futureTime, Status: "OPEN"},
				{AdvertisedStartTime: futureTime, Status: "OPEN"},
			},
		},
		"Multiple races with past times": {
			Input: []*racing.Race{
				{AdvertisedStartTime: pastTime},
				{AdvertisedStartTime: pastTime},
			},
			ExpectedRaces: []*racing.Race{
				{AdvertisedStartTime: pastTime, Status: "CLOSED"},
				{AdvertisedStartTime: pastTime, Status: "CLOSED"},
			},
		},
		"Multiple races with differing times": {
			Input: []*racing.Race{
				{AdvertisedStartTime: futureTime},
				{AdvertisedStartTime: pastTime},
			},
			ExpectedRaces: []*racing.Race{
				{AdvertisedStartTime: futureTime, Status: "OPEN"},
				{AdvertisedStartTime: pastTime, Status: "CLOSED"},
			},
		},
		"Race with no AdvertisedStartTime": {
			Input: []*racing.Race{
				{AdvertisedStartTime: nil},
			},
			ExpectedRaces: []*racing.Race{
				{AdvertisedStartTime: nil, Status: ""},
			},
		},
		"Multiple races with differing times and missing times": {
			Input: []*racing.Race{
				{AdvertisedStartTime: futureTime},
				{AdvertisedStartTime: pastTime},
				{AdvertisedStartTime: nil},
			},
			ExpectedRaces: []*racing.Race{
				{AdvertisedStartTime: futureTime, Status: "OPEN"},
				{AdvertisedStartTime: pastTime, Status: "CLOSED"},
				{AdvertisedStartTime: nil, Status: ""},
			},
		},
	}

	// Run tests
	for name, cfg := range tests {
		t.Run(
			name,
			func(cfg addStatusConfig) func(t *testing.T) {
				return func(*testing.T) {
					got := addStatus(cfg.Input)
					assert.Equal(t, cfg.ExpectedRaces, got)
				}
			}(cfg))
	}
}

// Instantiates an in-memory DB with testTableDefinition for unit tests
func memoryDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Errorf("unable to instantiate in-memory sqlite DB")
	}

	// Create the testing race table
	statement, _ := db.Prepare(testTableDefinition)
	_, _ = statement.Exec()

	return db
}

// Go doesn't allow for pointers to literals; this is a generic function used as a workaround
func pointerTo[T any](p T) *T {
	return &p
}
