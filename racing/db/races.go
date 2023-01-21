package db

import (
	"database/sql"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"git.neds.sh/matty/entain/racing/proto/racing"
	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// RacesRepo provides repository access to races.
type RacesRepo interface {
	// Init will initialise our races repository.
	Init() error

	// List will return a list of races.
	List(filter *racing.ListRacesRequestFilter, order *racing.ListRacesRequestOrder) ([]*racing.Race, error)
}

type racesRepo struct {
	db   *sql.DB
	init sync.Once
}

// NewRacesRepo creates a new races repository.
func NewRacesRepo(db *sql.DB) RacesRepo {
	return &racesRepo{db: db}
}

// Init prepares the race repository dummy data.
func (r *racesRepo) Init() error {
	var err error

	r.init.Do(func() {
		// For test/example purposes, we seed the DB with some dummy races.
		err = r.seed()
	})

	return err
}

func (r *racesRepo) List(filter *racing.ListRacesRequestFilter, order *racing.ListRacesRequestOrder) ([]*racing.Race, error) {
	var (
		err   error
		query string
		args  []interface{}
	)

	query = getRaceQueries()[racesList]

	query, args = r.applyFilter(query, filter)

	query = r.applyOrder(query, order)

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	return r.scanRaces(rows)
}

// Allows for a ListRaces RPC to be ordered by a user-provided field, in a user-provided direction. Validates the user
// provided field against columns returned by the DB.
func (r *racesRepo) applyOrder(query string, order *racing.ListRacesRequestOrder) string {
	// Return immediately if not in request
	if order == nil {
		return query
	}

	// Determine a list of columns upon which you can order by; the validity of which should be determined by the DB
	validColumns := make(map[string]bool)
	columnQuery := getRaceQueries()[racesColumnsList]
	rows, err := r.db.Query(columnQuery)
	if err != nil {
		log.Print("failed to get column names for ListRaces, continuing without")
		return query
	}

	// Iterate over the rows returned from the DB and add them to a list
	for rows.Next() {
		var columnName string
		err := rows.Scan(&columnName)
		if err != nil {
			log.Print("failed to parse column names for ListRaces, continuing without")
			return query
		}
		validColumns[columnName] = true
	}

	// Append user selected field if it's valid (i.e. was one of the columns returned earlier)
	if _, ok := validColumns[order.GetField()]; !ok {
		return query
	}
	query += " ORDER BY " + order.GetField()

	// Append user selected direction if it's valid and provided
	if order.Direction != nil {
		direction := strings.ToUpper(order.GetDirection())
		switch direction {
		case "ASC":
			query += " ASC"
		case "DESC":
			query += " DESC"
		}
	}

	return query
}

func (r *racesRepo) applyFilter(query string, filter *racing.ListRacesRequestFilter) (string, []interface{}) {
	var (
		clauses []string
		args    []interface{}
	)

	if filter == nil {
		return query, args
	}

	if len(filter.MeetingIds) > 0 {
		clauses = append(clauses, "meeting_id IN ("+strings.Repeat("?,", len(filter.MeetingIds)-1)+"?)")

		for _, meetingID := range filter.MeetingIds {
			args = append(args, meetingID)
		}
	}

	if filter.Visible != nil {
		clauses = append(clauses, "visible = "+strconv.FormatBool(filter.GetVisible()))
	}

	if len(clauses) != 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}

	return query, args
}

func (m *racesRepo) scanRaces(
	rows *sql.Rows,
) ([]*racing.Race, error) {
	var races []*racing.Race

	for rows.Next() {
		var race racing.Race
		var advertisedStart time.Time

		if err := rows.Scan(&race.Id, &race.MeetingId, &race.Name, &race.Number, &race.Visible, &advertisedStart); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}

			return nil, err
		}

		ts := timestamppb.New(advertisedStart)

		race.AdvertisedStartTime = ts

		races = append(races, &race)
	}

	races = addStatus(races)

	return races, nil
}

// Iterates through a set of provided races and calculates the value for the `status` field based upon whether a races
// advertisedStartTime has passed or not.
func addStatus(races []*racing.Race) []*racing.Race {
	for _, race := range races {

		// If an AdvertisedStartTime isn't set, avoid determining the status
		if race.AdvertisedStartTime == nil {
			continue
		}

		// If the start time is in the future it's "OPEN", otherwise "CLOSED"
		if race.AdvertisedStartTime.AsTime().After(time.Now()) {
			race.Status = "OPEN"
		} else {
			race.Status = "CLOSED"
		}
	}

	return races
}
