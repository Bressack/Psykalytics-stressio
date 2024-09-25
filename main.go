package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"time"
)

type EventRow struct {
	Id         string    `json:"id"`
	Type       string    `json:"type"`
	Session_id string    `json:"session_id"`
	Timestamp  time.Time `json:"timestamp"`
	Sint       int32     `json:"sint"`
	Lint       int64     `json:"lint"`
	Sstr       string    `json:"sstr"`
	Lstr       string    `json:"lstr"`
}

type DTOSessionId struct {
	Session_id string `json:"session_id"`
}

type DTOSession struct {
	Id     string     `json:"id"`
	Events []EventRow `json:"events"`
}

type DTOEvent struct {
	Type string `json:"type"`
	Sint int32  `json:"sint"`
	Lint int64  `json:"lint"`
	Sstr string `json:"sstr"`
	Lstr string `json:"lstr"`
}

func EmptyEvent() DTOEvent {
	return DTOEvent{"", 0, 0, "", ""}
}

type DTOCheckEvent struct {
	Timestamp time.Time `json:"timestamp"`
	Sstr      string    `json:"sstr"`
}

/////////////////////////////////////////

type SessionConfig struct {
	EventCount uint32 `json:"eventCount"`
}

type EventStats struct {
	Bsend    time.Time `json:"bsend"`
	Asend    time.Time `json:"asend"`
	DBTime   time.Time `json:"dbtime"`
	Brecv    time.Time `json:"brecv"`
	Arecv    time.Time `json:"arecv"`
	Attempts uint32    `json:"attempts"`
	Err      error     `json:"err"`
}

type Session struct {
	Id         int
	client     *http.Client
	Config     SessionConfig
	Events     []EventStats
	Session_id string
	Err        error
}

var BASEURL string = "http://192.168.109.149:8080"

///////////////////////////////////////////////////
// ORM

func LogError(err error) {
	_, _, line, _ := runtime.Caller(1)
	log.Printf("%d: %s\n", line, err.Error())
}

func (s *Session) GetEvent(stats *EventStats, event_id int32) {
	stats.Brecv = time.Now()
	uri := fmt.Sprintf("/session/%s/event/%d", s.Session_id, event_id)
	stats.Attempts = 0
	for {
		resp, err := http.Get(BASEURL + uri)
		if err != nil {
			stats.Err = err
			break
		}
		if resp.StatusCode == 200 {
			buf := new(bytes.Buffer)
			_, err = buf.ReadFrom(resp.Body)
			if err != nil {
				stats.Err = err
				break
			}
			var ce DTOCheckEvent
			err := json.Unmarshal(buf.Bytes(), &ce)
			stats.DBTime = ce.Timestamp
			if err != nil {
				stats.Err = err
			}
			break
		}
		stats.Attempts++
		if stats.Attempts >= 10 {
			stats.Err = errors.New("max attempts reached")
			break
		}
	}
	stats.Arecv = time.Now()
}

func (s *Session) SendEvent(data DTOEvent, session_id string) (string, error) {
	datastr, err := json.Marshal(data)
	if err != nil {
		log.Println(err)
		return "", errors.New("unable to marshal data")
	}
	request, err := http.NewRequest("POST", BASEURL+"/send", bytes.NewBuffer(datastr))
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/json; charset=UTF-8")
	if session_id != "" {
		request.Header.Set("session_id", session_id)
	}
	resp, err := s.client.Do(request)
	if err != nil {
		return "", err
	}
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(resp.Body)
	if err != nil {
		return "", err
	}
	if buf.Len() > 0 {
		var dtosession_id DTOSessionId
		err = json.Unmarshal(buf.Bytes(), &dtosession_id)
		if err != nil {
			return "", err
		}
		return dtosession_id.Session_id, nil
	}
	return "", nil
}

///////////////////////////////////////////////////

// Simulate a Session with a random number of events
func (s *Session) Run() {
	var err error
	var wg sync.WaitGroup
	s.Session_id, err = s.SendEvent(EmptyEvent(), "")
	s.Events = make([]EventStats, s.Config.EventCount)
	if err != nil {
		s.Err = err
		return
	}
	for reqno := range s.Config.EventCount {
		func(id int32) {
			var stats *EventStats = &s.Events[id]
			stats.Bsend = time.Now()
			_, err := s.SendEvent(DTOEvent{
				"event",
				id,
				0,
				"event_" + strconv.Itoa(int(id)),
				"",
			}, s.Session_id)
			stats.Asend = time.Now()
			if err != nil {
				stats.Err = err
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				s.GetEvent(stats, id)
			}()
		}(int32(reqno))
	}
	wg.Wait()
}

func main() {
	workerCount := 50
	var wg sync.WaitGroup
	var sessions []Session = make([]Session, workerCount)
	for i := range workerCount {
		sessions[i] = Session{
			i,
			&http.Client{CheckRedirect: nil},
			// SessionConfig{uint32((rand.Float32() * 50) + 50)},
			SessionConfig{uint32(197 - 40)},
			nil,
			"",
			nil,
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			sessions[i].Run()
		}()
	}
	wg.Wait()
	for i := range sessions {
		fmt.Printf("%s | ", sessions[i].Session_id)
		for j := range sessions[i].Events {
			if sessions[i].Events[j].Err != nil {
				fmt.Printf("\033[41;37;1m-\033[0m")
			} else {
				fmt.Printf("\033[42;30m+\033[0m")
			}
		}
		fmt.Printf("\033[0m\n")
	}
}
