package kaplex

import (
	"context"
	"encoding/json"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
	"strings"
	"time"
)

func Send(event *Event) error {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(config.Url),
		Topic:                  event.Topic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
		Async:                  true,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				log.Err(err).Msg("error when writing message to kafka")
			}
		},
	}
	defer closeWriter(w)

	message := kafka.Message{
		Key:   []byte(event.ID),
		Value: event.AsJson(),
		Time:  time.Now(),
	}
	return w.WriteMessages(context.Background(), message)
}

func Read(topic string, f func(e Event)) {
	brokers := strings.Split(config.Url, ",")
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  config.ConsumerGroup,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	defer closeReader(r)
	log.Info().Msgf("start consuming kafka messages for topic %s", topic)
	for {
		msg, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Err(err).Msgf("error when consuming message for topic %s", topic)
		}

		var event Event
		err = json.Unmarshal(msg.Value, &event)
		if err != nil {
			log.Err(err).Msgf("error when resolving message content for topic %s", topic)
		}
		f(event)
	}
}

func closeWriter(w *kafka.Writer) {
	if err := w.Close(); err != nil {
		log.Fatal().Msg("could not close writer")
	}
}

func closeReader(r *kafka.Reader) {
	if err := r.Close(); err != nil {
		log.Fatal().Msg("could not close writer")
	}
}
