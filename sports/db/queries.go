package db

const (
	eventsColumnsList = "columnsList"
	eventsList        = "list"
)

func getEventsQueries() map[string]string {
	return map[string]string{
		eventsColumnsList: `SELECT name FROM pragma_table_info('events')`,
		eventsList: `
			SELECT 
				id, 
				home_team, 
				away_team, 
				venue_location, 
				visible, 
				advertised_start_time 
			FROM events
		`,
	}
}
