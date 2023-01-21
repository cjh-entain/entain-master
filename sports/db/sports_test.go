package db

import (
	"database/sql"
	"testing"
	"time"

	"git.neds.sh/matty/entain/sports/proto/sports"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	testTableDefinition = `CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, home_team TEXT, away_team TEXT, venue_location TEXT, visible INTEGER, advertised_start_time DATETIME)`
)

type applyFilterConfig struct {
	Filter        *sports.ListEventsRequestFilter
	ExpectedQuery string
	ExpectedArgs  []interface{}
}

// Validates .applyFilter by comparing query strings. As the modification occurs at the end of the query string, these
// unit tests are validating only the end of the query. This avoids an additional unnecessary dependency on
// getQueryStrings().
func Test_SportsRepo_applyFilter(t *testing.T) {

	tests := map[string]applyFilterConfig{
		"Base Case - No filters": {
			Filter:        &sports.ListEventsRequestFilter{},
			ExpectedQuery: "",
		},
		"Filter on home team name": {
			Filter: &sports.ListEventsRequestFilter{
				HomeTeam: pointerTo("Chicago Cubs"),
			},
			ExpectedArgs:  []interface{}{"Chicago Cubs"},
			ExpectedQuery: " WHERE home_team = ?",
		},
		"Filter on away team name": {
			Filter: &sports.ListEventsRequestFilter{
				AwayTeam: pointerTo("Miami Heat"),
			},
			ExpectedArgs:  []interface{}{"Miami Heat"},
			ExpectedQuery: " WHERE away_team = ?",
		},
		"Filter on venue location": {
			Filter: &sports.ListEventsRequestFilter{
				VenueLocation: pointerTo("Pennsylvania"),
			},
			ExpectedArgs:  []interface{}{"Pennsylvania"},
			ExpectedQuery: " WHERE venue_location = ?",
		},
		"Filter on Visible = true": {
			Filter: &sports.ListEventsRequestFilter{
				Visible: pointerTo(true),
			},
			ExpectedQuery: " WHERE visible = true",
		},
		"Filter on Visible = false": {
			Filter: &sports.ListEventsRequestFilter{
				Visible: pointerTo(false),
			},
			ExpectedQuery: " WHERE visible = false",
		},
		"Filter on multiple (away team name and venue location)": {
			Filter: &sports.ListEventsRequestFilter{
				AwayTeam:      pointerTo("San Francisco 49ers"),
				VenueLocation: pointerTo("Minnesota"),
			},
			ExpectedArgs:  []interface{}{"San Francisco 49ers", "Minnesota"},
			ExpectedQuery: " WHERE away_team = ? AND venue_location = ?",
		},
	}

	// Create DB & SportsRepo struct
	sportsDB := memoryDB(t)
	defer sportsDB.Close()
	sportsRepo := &sportsRepo{
		db: sportsDB,
	}

	// Run tests
	for name, cfg := range tests {
		t.Run(
			name,
			func(cfg applyFilterConfig) func(t *testing.T) {
				return func(*testing.T) {
					gotQuery, gotArgs := sportsRepo.applyFilter("", cfg.Filter)
					assert.Equal(t, cfg.ExpectedArgs, gotArgs)
					assert.Equal(t, cfg.ExpectedQuery, gotQuery)
				}
			}(cfg))
	}
}

type applyOrderConfig struct {
	Order         *sports.ListEventsRequestOrder
	ExpectedQuery string
}

// Validates .applyOrder by comparing query strings. As with .applyFilter, these unit tests are validating only the end
// of the query string and any associated arguments.
func Test_SportsRepo_applyOrder(t *testing.T) {

	tests := map[string]applyOrderConfig{
		"Base case - No order provided": {
			Order:         nil,
			ExpectedQuery: "",
		},
		"Order provided for invalid field, no direction": {
			Order: &sports.ListEventsRequestOrder{
				Field:     "unknown",
				Direction: nil,
			},
			ExpectedQuery: "",
		},
		"Order provided for invalid field with direction resulting in no changes": {
			Order: &sports.ListEventsRequestOrder{
				Field:     "unknown",
				Direction: pointerTo("ASC"),
			},
			ExpectedQuery: "",
		},
		"Order provided for valid field, no direction": {
			Order: &sports.ListEventsRequestOrder{
				Field:     "home_team",
				Direction: nil,
			},
			ExpectedQuery: " ORDER BY home_team",
		},
		"Order provided for valid field, ASC direction": {
			Order: &sports.ListEventsRequestOrder{
				Field:     "home_team",
				Direction: pointerTo("ASC"),
			},
			ExpectedQuery: " ORDER BY home_team ASC",
		},
		"Order provided for valid field, DESC direction": {
			Order: &sports.ListEventsRequestOrder{
				Field:     "home_team",
				Direction: pointerTo("DESC"),
			},
			ExpectedQuery: " ORDER BY home_team DESC",
		},
		"Order provided for valid field, invalid direction": {
			Order: &sports.ListEventsRequestOrder{
				Field:     "home_team",
				Direction: pointerTo("INCORRECT"),
			},
			ExpectedQuery: " ORDER BY home_team",
		},
	}

	// Create DB & SportsRepo struct
	sportsDB := memoryDB(t)
	defer sportsDB.Close()
	sportsRepo := &sportsRepo{
		db: sportsDB,
	}

	// Run tests
	for name, cfg := range tests {
		t.Run(
			name,
			func(cfg applyOrderConfig) func(t *testing.T) {
				return func(*testing.T) {
					gotQuery := sportsRepo.applyOrder("", cfg.Order)
					assert.Equal(t, cfg.ExpectedQuery, gotQuery)
				}
			}(cfg))
	}
}

type addStatusConfig struct {
	Input          []*sports.Event
	ExpectedSports []*sports.Event
}

// Validates the calculation of derived fields (name and status)
func Test_addStatus(t *testing.T) {

	const (
		homeTeam  = "home"
		awayTeam  = "away"
		eventName = "away vs home"
	)

	var (
		futureTime = timestamppb.New(time.Now().Add(time.Hour * 24))
		pastTime   = timestamppb.New(time.Now().Add(-time.Hour * 24))
	)

	tests := map[string]addStatusConfig{
		"No input": {
			Input:          nil,
			ExpectedSports: nil,
		},
		"Single event with future time": {
			Input: []*sports.Event{
				{
					AdvertisedStartTime: futureTime,
					HomeTeam:            homeTeam,
					AwayTeam:            awayTeam,
				},
			},
			ExpectedSports: []*sports.Event{
				{
					AdvertisedStartTime: futureTime,
					Status:              "OPEN",
					HomeTeam:            homeTeam,
					AwayTeam:            awayTeam,
					Name:                eventName,
				},
			},
		},
		"Single event with past time": {
			Input: []*sports.Event{
				{
					AdvertisedStartTime: pastTime,
					HomeTeam:            homeTeam,
					AwayTeam:            awayTeam,
				},
			},
			ExpectedSports: []*sports.Event{
				{
					AdvertisedStartTime: pastTime,
					Status:              "CLOSED",
					HomeTeam:            homeTeam,
					AwayTeam:            awayTeam,
					Name:                eventName,
				},
			},
		},
		"Multiple events with future times": {
			Input: []*sports.Event{
				{
					AdvertisedStartTime: futureTime,
					HomeTeam:            homeTeam,
					AwayTeam:            awayTeam,
				},
				{
					AdvertisedStartTime: futureTime,
					HomeTeam:            homeTeam,
					AwayTeam:            awayTeam,
				},
			},
			ExpectedSports: []*sports.Event{
				{
					AdvertisedStartTime: futureTime,
					Status:              "OPEN",
					HomeTeam:            homeTeam,
					AwayTeam:            awayTeam,
					Name:                eventName,
				},
				{
					AdvertisedStartTime: futureTime,
					Status:              "OPEN",
					HomeTeam:            homeTeam,
					AwayTeam:            awayTeam,
					Name:                eventName,
				},
			},
		},
		"Multiple events with past times": {
			Input: []*sports.Event{
				{
					AdvertisedStartTime: pastTime,
					HomeTeam:            homeTeam,
					AwayTeam:            awayTeam,
				},
				{
					AdvertisedStartTime: pastTime,
					HomeTeam:            homeTeam,
					AwayTeam:            awayTeam,
				},
			},
			ExpectedSports: []*sports.Event{
				{
					AdvertisedStartTime: pastTime,
					Status:              "CLOSED",
					HomeTeam:            homeTeam,
					AwayTeam:            awayTeam,
					Name:                eventName,
				},
				{
					AdvertisedStartTime: pastTime,
					Status:              "CLOSED",
					HomeTeam:            homeTeam,
					AwayTeam:            awayTeam,
					Name:                eventName,
				},
			},
		},
		"Multiple events with differing times": {
			Input: []*sports.Event{
				{
					AdvertisedStartTime: futureTime,
					HomeTeam:            homeTeam,
					AwayTeam:            awayTeam,
				},
				{
					AdvertisedStartTime: pastTime,
					HomeTeam:            homeTeam,
					AwayTeam:            awayTeam,
				},
			},
			ExpectedSports: []*sports.Event{
				{
					AdvertisedStartTime: futureTime,
					Status:              "OPEN",
					HomeTeam:            homeTeam,
					AwayTeam:            awayTeam,
					Name:                eventName,
				},
				{
					AdvertisedStartTime: pastTime,
					Status:              "CLOSED",
					HomeTeam:            homeTeam,
					AwayTeam:            awayTeam,
					Name:                eventName,
				},
			},
		},
		"Event with no AdvertisedStartTime": {
			Input: []*sports.Event{
				{
					AdvertisedStartTime: nil,
					HomeTeam:            homeTeam,
					AwayTeam:            awayTeam,
				},
			},
			ExpectedSports: []*sports.Event{
				{
					AdvertisedStartTime: nil,
					HomeTeam:            homeTeam,
					AwayTeam:            awayTeam,
					Name:                eventName,
				},
			},
		},
		"Multiple events with differing times and missing times": {
			Input: []*sports.Event{
				{
					AdvertisedStartTime: futureTime,
					HomeTeam:            homeTeam,
					AwayTeam:            awayTeam,
				},
				{
					AdvertisedStartTime: pastTime,
					HomeTeam:            homeTeam,
					AwayTeam:            awayTeam,
				},
				{
					AdvertisedStartTime: nil,
					HomeTeam:            homeTeam,
					AwayTeam:            awayTeam,
				},
			},
			ExpectedSports: []*sports.Event{
				{
					AdvertisedStartTime: futureTime,
					Status:              "OPEN",
					HomeTeam:            homeTeam,
					AwayTeam:            awayTeam,
					Name:                eventName,
				},
				{
					AdvertisedStartTime: pastTime,
					Status:              "CLOSED",
					HomeTeam:            homeTeam,
					AwayTeam:            awayTeam,
					Name:                eventName,
				},
				{
					AdvertisedStartTime: nil,
					Status:              "",
					HomeTeam:            homeTeam,
					AwayTeam:            awayTeam,
					Name:                eventName,
				},
			},
		},
		"Generates an event name": {
			Input: []*sports.Event{
				{HomeTeam: "Minnesota Timberwolves", AwayTeam: "Chicago Bulls"},
			},
			ExpectedSports: []*sports.Event{
				{
					Name:     "Chicago Bulls vs Minnesota Timberwolves",
					HomeTeam: "Minnesota Timberwolves",
					AwayTeam: "Chicago Bulls",
				},
			},
		},
	}

	// Run tests
	for name, cfg := range tests {
		t.Run(
			name,
			func(cfg addStatusConfig) func(t *testing.T) {
				return func(*testing.T) {
					got := addDerivedFields(cfg.Input)
					assert.Equal(t, cfg.ExpectedSports, got)
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

	// Create the testing event table
	statement, _ := db.Prepare(testTableDefinition)
	_, _ = statement.Exec()

	return db
}

// Go doesn't allow for pointers to literals; this is a generic function used as a workaround
func pointerTo[T any](p T) *T {
	return &p
}
