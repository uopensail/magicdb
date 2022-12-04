package status

import 
(
	"sync"
)
const (
	TableStatus_NotSet int32 = 0
	TableStatus_Serving int32 =1
	TableStatus_Loading int32 = 2
)

type EngineStatus struct{
	machine bool
	database bool
	tables map[string]int32
	locker sync.RWMutex
}

func (s *EngineStatus)SetMachineStatus(status bool){
	s.locker.Lock()
	defer s.locker.Unlock()
	s.machine = status
	if !status {
		s.tables = nil
	}
}

func (s *EngineStatus)SetDataBaseStatus(status bool){
	s.locker.Lock()
	defer s.locker.Unlock()
	s.database = status
	if !status {
		s.tables = nil
	}
}

func (s *EngineStatus)SetTableStatus(table string, status int32){
	s.locker.Lock()
	defer s.locker.Unlock()
	if s.tables != nil{
		s.tables[table] = status
	}
}

func (s *EngineStatus)UpdateTables(tables [] string){
	nTables:= make(map[string]int32, len(tables))
	s.locker.Lock()
	defer s.locker.Unlock()
	if s.tables == nil{
		for _,t:=range tables {
			nTables[t] = TableStatus_NotSet
		}
	}else{
		for _,t:=range tables {
			if v,ok:= s.tables[t];ok{
				nTables[t] = v
			}else{
				nTables[t] = TableStatus_NotSet
			}
		}
	}
	s.tables = nTables
}

func (s *EngineStatus)IsServing() bool {
	s.locker.RLock()
	defer s.locker.RUnlock()
	if !s.machine {
		return false
	}
	if !s.database {
		return false
	}

	for _,status :=  range s.tables{
		if status == TableStatus_loading {
			return false
		}
	}
	return true
}

var EngineStatusImp EngineStatus

func init(){
	EngineStatusImp.tables = make(map[string]int32)
	EngineStatusImp.machine = false
	EngineStatusImp.database = false
}
