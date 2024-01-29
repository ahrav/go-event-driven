package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/ThreeDotsLabs/go-event-driven/common/clients"
	"github.com/ThreeDotsLabs/go-event-driven/common/clients/receipts"
	"github.com/ThreeDotsLabs/go-event-driven/common/clients/spreadsheets"
	commonHTTP "github.com/ThreeDotsLabs/go-event-driven/common/http"
	"github.com/ThreeDotsLabs/go-event-driven/common/log"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/lithammer/shortuuid/v3"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type TicketsStatusRequest struct {
	Tickets []TicketStatus `json:"tickets"`
}

type Status string

const (
	StatusConfirmed Status = "confirmed"
	StatusCanceled  Status = "canceled"
)

type TicketStatus struct {
	TicketID      string `json:"ticket_id"`
	Status        Status `json:"status"`
	Price         Money  `json:"price"`
	CustomerEmail string `json:"customer_email"`
}

type Money struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type EventHeader struct {
	ID          string    `json:"id"`
	PublishedAt time.Time `json:"published_at"`
}

type TicketBookingConfirmed struct {
	Header        EventHeader `json:"header"`
	TicketID      string      `json:"ticket_id"`
	CustomerEmail string      `json:"customer_email"`
	Price         Money       `json:"price"`
}

type TicketBookingCanceled struct {
	Header        EventHeader `json:"header"`
	TicketID      string      `json:"ticket_id"`
	CustomerEmail string      `json:"customer_email"`
	Price         Money       `json:"price"`
}

const (
	correlationIDHeader      = "Correlation-ID"
	correlationIDMetadataKey = "correlation_id"

	ticketBookingConfirmed = "TicketBookingConfirmed"
	ticketBookingCanceled  = "TicketBookingCanceled"
)

func main() {
	log.Init(logrus.InfoLevel)

	newClients, err := clients.NewClients(
		os.Getenv("GATEWAY_ADDR"),
		func(ctx context.Context, req *http.Request) error {
			req.Header.Set(correlationIDHeader, log.CorrelationIDFromContext(ctx))
			return nil
		},
	)
	if err != nil {
		panic(err)
	}

	receiptsClient := NewReceiptsClient(newClients)
	spreadsheetsClient := NewSpreadsheetsClient(newClients)

	watermillLogger := log.NewWatermill(logrus.NewEntry(logrus.StandardLogger()))

	rdb := redis.NewClient(&redis.Options{Addr: os.Getenv("REDIS_ADDR")})

	pub, err := redisstream.NewPublisher(redisstream.PublisherConfig{
		Client: rdb,
	}, watermillLogger)
	if err != nil {
		panic(err)
	}

	issueReceiptSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client:        rdb,
		ConsumerGroup: "issue-receipt",
	}, watermillLogger)
	if err != nil {
		panic(err)
	}

	appendToTrackerSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client:        rdb,
		ConsumerGroup: "append-to-tracker",
	}, watermillLogger)
	if err != nil {
		panic(err)
	}

	cancelTicketSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client:        rdb,
		ConsumerGroup: "cancel-ticket",
	}, watermillLogger)
	if err != nil {
		panic(err)
	}

	e := commonHTTP.NewEcho()

	e.GET("/health", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	e.POST("/tickets-status", func(c echo.Context) error {
		var request TicketsStatusRequest
		if err := c.Bind(&request); err != nil {
			return err
		}

		for _, ticket := range request.Tickets {
			switch ticket.Status {
			case StatusConfirmed:
				event := TicketBookingConfirmed{
					Header:        NewHeader(),
					TicketID:      ticket.TicketID,
					CustomerEmail: ticket.CustomerEmail,
					Price:         ticket.Price,
				}

				payload, err := json.Marshal(event)
				if err != nil {
					return err
				}

				msg := message.NewMessage(watermill.NewUUID(), payload)
				msg.Metadata.Set(correlationIDMetadataKey, c.Request().Header.Get(correlationIDHeader))
				msg.Metadata.Set("type", ticketBookingConfirmed)

				err = pub.Publish(ticketBookingConfirmed, msg)
				if err != nil {
					return err
				}
			case StatusCanceled:
				event := TicketBookingCanceled{
					Header:        NewHeader(),
					TicketID:      ticket.TicketID,
					CustomerEmail: ticket.CustomerEmail,
					Price:         ticket.Price,
				}

				payload, err := json.Marshal(event)
				if err != nil {
					return err
				}

				msg := message.NewMessage(watermill.NewUUID(), payload)
				msg.Metadata.Set(correlationIDMetadataKey, c.Request().Header.Get(correlationIDHeader))
				msg.Metadata.Set("type", ticketBookingCanceled)

				err = pub.Publish(ticketBookingCanceled, msg)
				if err != nil {
					return err
				}
			default:
				return fmt.Errorf("unknown ticket status: %s", ticket.Status)
			}
		}

		return c.NoContent(http.StatusOK)
	})

	router, err := message.NewRouter(message.RouterConfig{}, watermillLogger)
	if err != nil {
		panic(err)
	}

	router.AddMiddleware(func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) (events []*message.Message, err error) {
			ctx := msg.Context()

			reqCorrelationID := msg.Metadata.Get(correlationIDMetadataKey)
			if reqCorrelationID == "" {
				reqCorrelationID = shortuuid.New()
			}

			ctx = log.ContextWithCorrelationID(ctx, reqCorrelationID)
			ctx = log.ToContext(ctx, logrus.WithFields(logrus.Fields{"correlation_id": reqCorrelationID}))

			msg.SetContext(ctx)

			return h(msg)
		}
	})
	router.AddMiddleware(LoggingMiddleware)
	router.AddMiddleware(middleware.Retry{
		MaxRetries:      10,
		InitialInterval: time.Millisecond * 100,
		MaxInterval:     time.Second,
		Multiplier:      2,
		Logger:          watermillLogger,
	}.Middleware)

	router.AddNoPublisherHandler(
		"issue_receipt",
		ticketBookingConfirmed,
		issueReceiptSub,
		func(msg *message.Message) error {
			var event TicketBookingConfirmed
			if msg.UUID == "2beaf5bc-d5e4-4653-b075-2b36bbf28949" {
				return nil
			}

			if msg.Metadata.Get("type") != ticketBookingConfirmed {
				return nil
			}

			err := json.Unmarshal(msg.Payload, &event)
			if err != nil {
				return err
			}

			return receiptsClient.IssueReceipt(msg.Context(), IssueReceiptRequest{
				TicketID: event.TicketID,
				Price: Money{
					Amount:   event.Price.Amount,
					Currency: event.Price.Currency,
				},
			})
		},
	)

	router.AddNoPublisherHandler(
		"print_ticket",
		ticketBookingConfirmed,
		appendToTrackerSub,
		func(msg *message.Message) error {
			var event TicketBookingConfirmed
			if msg.UUID == "2beaf5bc-d5e4-4653-b075-2b36bbf28949" {
				return nil
			}
			if msg.Metadata.Get("type") != ticketBookingConfirmed {
				return nil
			}

			err := json.Unmarshal(msg.Payload, &event)
			if err != nil {
				return err
			}

			return spreadsheetsClient.AppendRow(
				msg.Context(),
				"tickets-to-print",
				[]string{event.TicketID, event.CustomerEmail, event.Price.Amount, event.Price.Currency},
			)
		},
	)

	router.AddNoPublisherHandler(
		"cancel_ticket",
		ticketBookingCanceled,
		cancelTicketSub,
		func(msg *message.Message) error {
			var event TicketBookingCanceled
			if msg.Metadata.Get("type") != ticketBookingCanceled {
				return nil
			}

			err := json.Unmarshal(msg.Payload, &event)
			if err != nil {
				return err
			}

			return spreadsheetsClient.AppendRow(
				msg.Context(),
				"tickets-to-refund",
				[]string{event.TicketID, event.CustomerEmail, event.Price.Amount, event.Price.Currency},
			)
		},
	)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	errgrp, ctx := errgroup.WithContext(ctx)

	errgrp.Go(func() error {
		return router.Run(ctx)
	})

	errgrp.Go(func() error {
		// we don't want to start HTTP server before Watermill router. (so service won't be healthy before it's ready)
		<-router.Running()

		err := e.Start(":8080")

		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}

		return nil
	})

	errgrp.Go(func() error {
		<-ctx.Done()
		return e.Shutdown(context.Background())
	})

	if err := errgrp.Wait(); err != nil {
		panic(err)
	}
}

func LoggingMiddleware(next message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) (msgs []*message.Message, err error) {
		logger := log.FromContext(msg.Context())
		defer func() {
			if err != nil {
				logger.WithField("error", err).WithField("message_uuid", msg.UUID).Info("Message handling error")
			}
		}()

		logger.WithField("message_uuid", msg.UUID).Info("Handling a message")

		return next(msg)
	}
}

func NewHeader() EventHeader {
	return EventHeader{ID: uuid.NewString(), PublishedAt: time.Now().UTC()}
}

type ReceiptsClient struct{ clients *clients.Clients }

func NewReceiptsClient(clients *clients.Clients) ReceiptsClient {
	return ReceiptsClient{clients: clients}
}

type IssueReceiptRequest struct {
	TicketID string
	Price    Money
}

func (c ReceiptsClient) IssueReceipt(ctx context.Context, request IssueReceiptRequest) error {
	body := receipts.PutReceiptsJSONRequestBody{
		TicketId: request.TicketID,
		Price: receipts.Money{
			MoneyAmount:   request.Price.Amount,
			MoneyCurrency: request.Price.Currency,
		},
	}

	receiptsResp, err := c.clients.Receipts.PutReceiptsWithResponse(ctx, body)
	if err != nil {
		return err
	}
	if receiptsResp.StatusCode() != http.StatusOK {
		return fmt.Errorf("unexpected status code: %v", receiptsResp.StatusCode())
	}

	return nil
}

type SpreadsheetsClient struct{ clients *clients.Clients }

func NewSpreadsheetsClient(clients *clients.Clients) SpreadsheetsClient {
	return SpreadsheetsClient{clients: clients}
}

func (c SpreadsheetsClient) AppendRow(ctx context.Context, spreadsheetName string, row []string) error {
	request := spreadsheets.PostSheetsSheetRowsJSONRequestBody{
		Columns: row,
	}

	sheetsResp, err := c.clients.Spreadsheets.PostSheetsSheetRowsWithResponse(ctx, spreadsheetName, request)
	if err != nil {
		return err
	}
	if sheetsResp.StatusCode() != http.StatusOK {
		return fmt.Errorf("unexpected status code: %v", sheetsResp.StatusCode())
	}

	return nil
}
