package db

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"git.neds.sh/matty/entain/sports/proto/sports"
	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SportsRepo provides repository access to sports events.
type SportsRepo interface {
	// Init will initialise our sports events repository.
	Init() error

	// List will return a list of sports events.
	List(filter *sports.ListEventsRequestFilter, order *sports.ListEventsRequestOrder) ([]*sports.Event, error)
}

type sportsRepo struct {
	db   *sql.DB
	init sync.Once
}

// NewSportsRepo creates a new sports events repository.
func NewSportsRepo(db *sql.DB) SportsRepo {
	return &sportsRepo{db: db}
}

// Init prepares the sports repository dummy data.
func (s *sportsRepo) Init() error {
	var err error

	s.init.Do(func() {
		// For test/example purposes, we seed the DB with some dummy events.
		err = s.seed()
	})

	return err
}

func (s *sportsRepo) List(filter *sports.ListEventsRequestFilter, order *sports.ListEventsRequestOrder) ([]*sports.Event, error) {
	var (
		err   error
		query string
		args  []interface{}
	)

	query = getEventsQueries()[eventsList]

	query, args = s.applyFilter(query, filter)

	query = s.applyOrder(query, order)

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	return s.scanEvents(rows)
}

// Allows for a ListEvents RPC to be ordered by a user-provided field, in a user-provided direction. Validates the user
// provided field against columns returned by the DB.
func (s *sportsRepo) applyOrder(query string, order *sports.ListEventsRequestOrder) string {
	// Return immediately if not in request
	if order == nil {
		return query
	}

	// Determine a list of columns upon which you can order by; the validity of which should be determined by the DB
	validColumns := make(map[string]bool)
	columnQuery := getEventsQueries()[eventsColumnsList]
	rows, err := s.db.Query(columnQuery)
	if err != nil {
		log.Print("failed to get column names for ListEvents, continuing without")
		return query
	}

	// Iterate over the rows returned from the DB and add them to a list
	for rows.Next() {
		var columnName string
		err := rows.Scan(&columnName)
		if err != nil {
			log.Print("failed to parse column names for ListEvents, continuing without")
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

func (s *sportsRepo) applyFilter(query string, filter *sports.ListEventsRequestFilter) (string, []interface{}) {
	var (
		clauses []string
		args    []interface{}
	)

	if filter == nil {
		return query, args
	}

	if filter.HomeTeam != nil {
		clauses = append(clauses, "home_team = ?")
		args = append(args, filter.GetHomeTeam())
	}

	if filter.AwayTeam != nil {
		clauses = append(clauses, "away_team = ?")
		args = append(args, filter.GetAwayTeam())
	}

	if filter.VenueLocation != nil {
		clauses = append(clauses, "venue_location = ?")
		args = append(args, filter.GetVenueLocation())
	}

	if filter.Visible != nil {
		clauses = append(clauses, "visible = "+strconv.FormatBool(filter.GetVisible()))
	}

	if len(clauses) != 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}

	return query, args
}

// Iterates through a set of provided events and calculates derived fields (name and status)
func (s *sportsRepo) addDerivedFields(events []*sports.Event) []*sports.Event {
	for _, event := range events {

		// Generate the event name from the awayTeam and homeTeam names
		event.Name = fmt.Sprintf("%s vs %s", event.GetAwayTeam(), event.GetHomeTeam())

		// If the start time is in the future it's "OPEN", otherwise "CLOSED"
		if event.AdvertisedStartTime != nil {
			if event.AdvertisedStartTime.AsTime().After(time.Now()) {
				event.Status = "OPEN"
			} else {
				event.Status = "CLOSED"
			}
		}
	}

	return events
}

func (m *sportsRepo) scanEvents(
	rows *sql.Rows,
) ([]*sports.Event, error) {
	var events []*sports.Event

	for rows.Next() {
		var event sports.Event
		var advertisedStart time.Time

		if err := rows.Scan(&event.Id, &event.HomeTeam, &event.AwayTeam, &event.VenueLocation, &event.Visible, &advertisedStart); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}

			return nil, err
		}

		ts := timestamppb.New(advertisedStart)

		event.AdvertisedStartTime = ts

		events = append(events, &event)
	}

	events = m.addDerivedFields(events)

	return events, nil
}
