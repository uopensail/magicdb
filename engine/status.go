package engine

import (
	"sync"
)

const (
	TableStatus_NotSet  int32 = 0
	TableStatus_Serving int32 = 1
	TableStatus_Loading int32 = 2
)

type EngineStatus struct {
	Tables map[string]int32
	Locker sync.RWMutex
}

func NewEngineStatus() *EngineStatus {
	return &EngineStatus{
		Tables: make(map[string]int32),
	}
}

func (s *EngineStatus) SetTableStatus(table string, status int32) {
	s.Locker.Lock()
	defer s.Locker.Unlock()
	if s.Tables != nil {
		s.Tables[table] = status
	}
}

func (s *EngineStatus) AddTable(table string) {
	s.Locker.Lock()
	defer s.Locker.Unlock()
	s.Tables[table] = TableStatus_NotSet
}

func (s *EngineStatus) DelTable(table string) {
	s.Locker.Lock()
	defer s.Locker.Unlock()
	delete(s.Tables, table)
}

func (s *EngineStatus) IsServing() bool {
	s.Locker.RLock()
	defer s.Locker.RUnlock()

	for _, status := range s.Tables {
		if status == TableStatus_Loading {
			return false
		}
	}
	return true
}
