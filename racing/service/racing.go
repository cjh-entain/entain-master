package service

import (
	"git.neds.sh/matty/entain/racing/db"
	"git.neds.sh/matty/entain/racing/proto/racing"
	"golang.org/x/net/context"
)

type Racing interface {
	// ListRaces will return a collection of races.
	ListRaces(ctx context.Context, in *racing.ListRacesRequest) (*racing.ListRacesResponse, error)
	// GetRace will return a singular race based upon a provided id
	GetRace(ctx context.Context, in *racing.GetRaceRequest) (*racing.Race, error)
}

// racingService implements the Racing interface.
type racingService struct {
	racesRepo db.RacesRepo
}

// NewRacingService instantiates and returns a new racingService.
func NewRacingService(racesRepo db.RacesRepo) Racing {
	return &racingService{racesRepo}
}

func (s *racingService) ListRaces(ctx context.Context, in *racing.ListRacesRequest) (*racing.ListRacesResponse, error) {
	races, err := s.racesRepo.List(in.Filter, in.Order)
	if err != nil {
		return nil, err
	}

	return &racing.ListRacesResponse{Races: races}, nil
}

// GetRace Returns a single race event based upon a user-provided ID, or an error if the provided race cannot be found
func (s *racingService) GetRace(ctx context.Context, in *racing.GetRaceRequest) (*racing.Race, error) {
	race, err := s.racesRepo.GetByID(in.Id)
	if err != nil {
		return nil, err
	}

	return race, nil
}
