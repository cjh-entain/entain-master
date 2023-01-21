package db

import (
	"database/sql"
	"strings"
	"testing"

	"git.neds.sh/matty/entain/racing/proto/racing"
	"github.com/stretchr/testify/assert"
)

const (
	testTableDefinition = `CREATE TABLE IF NOT EXISTS races (id INTEGER PRIMARY KEY, meeting_id INTEGER, name TEXT, number INTEGER, visible INTEGER, advertised_start_time DATETIME)`
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

type applyOrderByConfig struct {
	OrderBy       *racing.ListRacesRequestOrderBy
	ExpectedQuery string
}

// Validates .applyOrderBy by comparing query strings
func Test_RacesRepo_applyOrderBy(t *testing.T) {

	tests := map[string]applyOrderByConfig{
		"Base case - No orderBy provided": {
			OrderBy:       nil,
			ExpectedQuery: "SELECT id, meeting_id, name, number, visible, advertised_start_time FROM races",
		},
		"OrderBy provided for invalid field, no direction": {
			OrderBy: &racing.ListRacesRequestOrderBy{
				Field:     "unknown",
				Direction: nil,
			},
			ExpectedQuery: "SELECT id, meeting_id, name, number, visible, advertised_start_time FROM races",
		},
		"OrderBy provided for invalid field with direction resulting in no changes": {
			OrderBy: &racing.ListRacesRequestOrderBy{
				Field:     "unknown",
				Direction: strPtr("ASC"),
			},
			ExpectedQuery: "SELECT id, meeting_id, name, number, visible, advertised_start_time FROM races",
		},
		"OrderBy provided for valid field, no direction": {
			OrderBy: &racing.ListRacesRequestOrderBy{
				Field:     "meeting_id",
				Direction: nil,
			},
			ExpectedQuery: "SELECT id, meeting_id, name, number, visible, advertised_start_time FROM races ORDER BY meeting_id",
		},
		"OrderBy provided for valid field, ASC direction": {
			OrderBy: &racing.ListRacesRequestOrderBy{
				Field:     "meeting_id",
				Direction: strPtr("ASC"),
			},
			ExpectedQuery: "SELECT id, meeting_id, name, number, visible, advertised_start_time FROM races ORDER BY meeting_id ASC",
		},
		"OrderBy provided for valid field, DESC direction": {
			OrderBy: &racing.ListRacesRequestOrderBy{
				Field:     "meeting_id",
				Direction: strPtr("DESC"),
			},
			ExpectedQuery: "SELECT id, meeting_id, name, number, visible, advertised_start_time FROM races ORDER BY meeting_id DESC",
		},
		"OrderBy provided for valid field, invalid direction": {
			OrderBy: &racing.ListRacesRequestOrderBy{
				Field:     "meeting_id",
				Direction: strPtr("INCORRECT"),
			},
			ExpectedQuery: "SELECT id, meeting_id, name, number, visible, advertised_start_time FROM races ORDER BY meeting_id",
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
			func(cfg applyOrderByConfig) func(t *testing.T) {
				return func(*testing.T) {
					query := getRaceQueries()[racesList]
					gotQueryTmp := racesRepo.applyOrderBy(query, cfg.OrderBy)
					gotQuery := replacer.Replace(gotQueryTmp)

					assert.Equal(t, cfg.ExpectedQuery, gotQuery)
				}
			}(cfg))
	}
}

// Go doesn't allow for pointers to literals; this is a workaround for the purpose of testing.
func boolPtr(b bool) *bool {
	return &b
}

// Go doesn't allow for pointers to literals; this is a workaround for the purpose of testing.
func strPtr(s string) *string {
	return &s
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
