package db

import (
	"database/sql"
	"strings"
	"testing"

	"git.neds.sh/matty/entain/racing/proto/racing"
	"github.com/stretchr/testify/assert"
)

type applyFilterConfig struct {
	Filter        *racing.ListRacesRequestFilter
	ExpectedQuery string
	ExpectedArgs  []interface{}
}

// Validates .applyFilter by comparing query strings
func Test_RacesRepo_applyFilter(t *testing.T) {

	tests := map[string]applyFilterConfig{
		"Base Case - No filters": {
			Filter:        &racing.ListRacesRequestFilter{},
			ExpectedQuery: "SELECT id, meeting_id, name, number, visible, advertised_start_time FROM races",
		},
		"Filter on single MeetingId": {
			Filter: &racing.ListRacesRequestFilter{
				MeetingIds: []int64{1},
			},
			ExpectedArgs:  []interface{}{int64(1)},
			ExpectedQuery: "SELECT id, meeting_id, name, number, visible, advertised_start_time FROM races WHERE meeting_id IN (?)",
		},
		"Filter on multiple MeetingId's": {
			Filter: &racing.ListRacesRequestFilter{
				MeetingIds: []int64{1, 2},
			},
			ExpectedArgs:  []interface{}{int64(1), int64(2)},
			ExpectedQuery: "SELECT id, meeting_id, name, number, visible, advertised_start_time FROM races WHERE meeting_id IN (?,?)",
		},
		"Filter on Visible = true": {
			Filter: &racing.ListRacesRequestFilter{
				Visible: boolPtr(true),
			},
			ExpectedQuery: "SELECT id, meeting_id, name, number, visible, advertised_start_time FROM races WHERE visible = true",
		},
		"Filter on Visible = false": {
			Filter: &racing.ListRacesRequestFilter{
				Visible: boolPtr(false),
			},
			ExpectedQuery: "SELECT id, meeting_id, name, number, visible, advertised_start_time FROM races WHERE visible = false",
		},
		"Filter on both MeetingId's and Visible": {
			Filter: &racing.ListRacesRequestFilter{
				MeetingIds: []int64{1, 2},
				Visible:    boolPtr(true),
			},
			ExpectedArgs:  []interface{}{int64(1), int64(2)},
			ExpectedQuery: "SELECT id, meeting_id, name, number, visible, advertised_start_time FROM races WHERE meeting_id IN (?,?) AND visible = true",
		},
	}

	// Create DB & RacesRepo struct
	racesDB := memoryDB(t)
	defer racesDB.Close()
	racesRepo := &racesRepo{
		db: racesDB,
	}

	// Used to remove any extraneous whitespace from the resulting query
	replacer := strings.NewReplacer("\n", "", "\t", "")

	// Run tests
	for name, cfg := range tests {
		t.Run(
			name,
			func(cfg applyFilterConfig) func(t *testing.T) {
				return func(*testing.T) {
					query := getRaceQueries()[racesList]
					gotQueryTmp, gotArgs := racesRepo.applyFilter(query, cfg.Filter)
					gotQuery := replacer.Replace(gotQueryTmp)

					assert.Equal(t, cfg.ExpectedArgs, gotArgs)
					assert.Equal(t, cfg.ExpectedQuery, gotQuery)
				}
			}(cfg))
	}
}

// Go doesn't allow for pointers to literals; this is a workaround for the purpose of testing.
func boolPtr(b bool) *bool {
	return &b
}

// Instantiates an in-memory DB for unit tests.
func memoryDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Errorf("unable to instantiate in-memory sqlite DB")
	}

	return db
}