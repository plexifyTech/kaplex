package kaplex

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type Event struct {
	ID     string `json:"id"`
	Source string `json:"source"`
	Type   string `json:"type"`
	Topic  string `json:"topic"`
	Data   []byte `json:"data"`
}

func NewEvent(topic string) *Event {
	return &Event{
		ID:     uuid.New().String(),
		Type:   fmt.Sprintf("%s.%s", "storm", topic),
		Source: "storm",
		Topic:  topic,
	}
}

func (e *Event) AsJson() []byte {
	byteEvent, err := json.Marshal(e)
	if err != nil {
		log.Err(err).Msg("error parsing data to json")
		return nil
	}
	return byteEvent
}
