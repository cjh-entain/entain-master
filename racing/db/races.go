package db

import (
	"database/sql"
	"errors"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"git.neds.sh/matty/entain/racing/proto/racing"
	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrCantFindID = errors.New("unable to locate a race with the provided ID")
)

// RacesRepo provides repository access to races.
type RacesRepo interface {
	// Init will initialise our races repository.
	Init() error

	// List will return a list of races.
	List(filter *racing.ListRacesRequestFilter, order *racing.ListRacesRequestOrder) ([]*racing.Race, error)

	// GetByID will return a single race based upon a provided id
	GetByID(id int64) (*racing.Race, error)
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

// GetByID Returns a singular race event, based upon the provided ID in the request
func (r *racesRepo) GetByID(id int64) (*racing.Race, error) {
	var (
		err   error
		query string
		args  []interface{}
	)

	query = getRaceQueries()[racesList]

	filter := &racing.ListRacesRequestFilter{Id: &id}

	query, args = r.applyFilter(query, filter)

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	res, err := r.scanRaces(rows)
	if err != nil {
		return nil, err
	}

	if len(res) == 0 {
		return nil, ErrCantFindID
	}

	return res[0], nil
}

// Allows for a ListRaces RPC to be ordered by a user-provided field, in a user-provided direction. Validates the user
// provided field against columns returned by the DB.
func (r *racesRepo) applyOrder(query string, order *racing.ListRacesRequestOrder) string {

	// Determines the direction for the order by
	var parseDirection = func(dir string) string {
		dir = strings.ToUpper(dir)
		switch dir {
		case "ASC":
			return " ASC"
		case "DESC":
			return " DESC"
		}
		return ""
	}

	// Return immediately if not in request
	if order == nil {
		return query
	}

	// Default order by if no field has been provided
	if order.Field == nil {
		query += " ORDER BY advertised_start_time" + parseDirection(order.GetDirection())
	}

	// As a field has been specified by the user, we need to determine if it's a valid and allowable choice
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
		query += parseDirection(order.GetDirection())
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

	if filter.Id != nil {
		clauses = append(clauses, "id = ?")
		args = append(args, filter.GetId())
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
