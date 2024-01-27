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
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type ReceiptsClient struct{ clients *clients.Clients }

func NewReceiptsClient(clients *clients.Clients) ReceiptsClient {
	return ReceiptsClient{clients: clients}
}

type Money struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type IssueReceiptRequest struct {
	TicketID string `json:"ticket_id"`
	Price    Money  `json:"price"`
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

type PrintTicketPayload struct {
	TicketID      string `json:"ticket_id"`
	CustomerEmail string `json:"customer_email"`
	Price         Money  `json:"price"`
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

type Status string

const (
	StatusConfirmed Status = "confirmed"
	StatusCanceled  Status = "canceled"
)

type Ticket struct {
	TicketId      string `json:"ticket_id"`
	Status        Status `json:"status"`
	CustomerEmail string `json:"customer_email"`
	Price         Money  `json:"price"`
}

type TicketsConfirmationRequest struct {
	Tickets []Ticket `json:"tickets"`
}

type Header struct {
	ID          string `json:"id"`
	PublishedAt string `json:"published_at"`
}

type TicketBookingConfirmedEvent struct {
	Header Header `json:"header"`
	Ticket
}

type TicketBookingCanceledEvent struct {
	Header Header `json:"header"`
	Ticket
}

const (
	// Topics.

	// TicketBookingConfirmed is a topic for events published when a ticket booking is confirmed.
	TicketBookingConfirmed = "TicketBookingConfirmed"
	// TicketBookingCanceled is a topic for events published when a ticket booking is canceled.
	TicketBookingCanceled = "TicketBookingCanceled"

	// Consumer groups.
	receiptsConsumerGroup = "receipts"
	trackerConsumerGroup  = "tracker"
	refundsConsumerGroup  = "refunds"
)

func main() {
	log.Init(logrus.InfoLevel)

	logger := log.NewWatermill(logrus.NewEntry(logrus.StandardLogger()))

	rdb := redis.NewClient(&redis.Options{Addr: os.Getenv("REDIS_ADDR")})

	c, err := clients.NewClients(os.Getenv("GATEWAY_ADDR"), nil)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	workerPool, ctx := errgroup.WithContext(ctx)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	issueReceiptSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client:        rdb,
		ConsumerGroup: receiptsConsumerGroup,
	}, logger)
	if err != nil {
		panic(err)
	}

	receiptsClient := NewReceiptsClient(c)
	workerPool.Go(func() error {
		processIssueReceipt(issueReceiptSub, router, receiptsClient.IssueReceipt)
		return nil
	})

	appendToTrackerSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client:        rdb,
		ConsumerGroup: trackerConsumerGroup,
	}, logger)
	if err != nil {
		panic(err)
	}

	refundSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client:        rdb,
		ConsumerGroup: refundsConsumerGroup,
	}, logger)
	if err != nil {
		panic(err)
	}

	spreadsheetsClient := NewSpreadsheetsClient(c)
	workerPool.Go(func() error {
		processTrackerAppend(appendToTrackerSub, router, spreadsheetsClient.AppendRow)
		return nil
	})

	workerPool.Go(func() error {
		processRefundsAppend(refundSub, router, spreadsheetsClient.AppendRow)
		return nil
	})

	workerPool.Go(func() error {
		return router.Run(ctx)
	})

	publisher, err := redisstream.NewPublisher(redisstream.PublisherConfig{Client: rdb}, logger)
	if err != nil {
		panic(err)
	}

	e := commonHTTP.NewEcho()
	e.GET("/health", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	e.POST("/tickets-status", func(c echo.Context) error {
		var request TicketsConfirmationRequest
		err := c.Bind(&request)
		if err != nil {
			return err
		}

		for _, ticket := range request.Tickets {
			var (
				topic string
				msg   *message.Message
				err   error
			)
			switch ticket.Status {
			case StatusConfirmed:
				var ticketBookingEvent []byte
				ticketBookingEvent, err = json.Marshal(TicketBookingConfirmedEvent{
					Header: Header{
						ID:          uuid.NewString(),
						PublishedAt: time.Now().Format(time.RFC3339),
					},
					Ticket: ticket,
				})
				if err != nil {
					logger.Error("error marshalling IssueReceiptRequest", err, watermill.LogFields{"ticket": ticket})
					continue
				}

				topic = TicketBookingConfirmed
				msg = message.NewMessage(watermill.NewUUID(), ticketBookingEvent)
			case StatusCanceled:
				var ticketCanceledEvent []byte
				ticketCanceledEvent, err = json.Marshal(TicketBookingCanceledEvent{
					Header: Header{
						ID:          uuid.NewString(),
						PublishedAt: time.Now().Format(time.RFC3339),
					},
					Ticket: ticket,
				})

				topic = TicketBookingCanceled
				msg = message.NewMessage(watermill.NewUUID(), ticketCanceledEvent)
			default:
				return fmt.Errorf("unknown ticket status: %v", ticket.Status)
			}
			if err != nil {
				logger.Error("error marshalling ticket event", err, watermill.LogFields{"ticket": ticket, "topic": topic})
			}

			return publisher.Publish(topic, msg)
		}

		return c.NoContent(http.StatusOK)
	})

	logrus.Info("Server starting...")

	workerPool.Go(func() error {
		<-router.Running()

		if err := e.Start(":8080"); err != nil && !errors.Is(err, http.ErrServerClosed) {
			panic(err)
		}
		return nil
	})

	workerPool.Go(func() error {
		<-ctx.Done()
		return e.Shutdown(ctx)
	})

	if err := workerPool.Wait(); err != nil {
		panic(err)
	}
}

func processIssueReceipt(
	sub message.Subscriber,
	router *message.Router,
	action func(ctx context.Context, request IssueReceiptRequest) error,
) {
	router.AddNoPublisherHandler(
		"issue_receipt_handler",
		TicketBookingConfirmed,
		sub,
		func(msg *message.Message) error {
			var req IssueReceiptRequest
			if err := json.Unmarshal(msg.Payload, &req); err != nil {
				return err
			}
			return action(msg.Context(), req)
		},
	)

}

func processTrackerAppend(
	sub message.Subscriber,
	router *message.Router,
	action func(ctx context.Context, spreadSheetName string, row []string) error,
) {
	const spreadsheetName = "tickets-to-print"

	router.AddNoPublisherHandler(
		"tracker_append_handler",
		TicketBookingConfirmed,
		sub,
		func(msg *message.Message) error {
			var payload PrintTicketPayload
			if err := json.Unmarshal(msg.Payload, &payload); err != nil {
				return err
			}
			return action(
				msg.Context(),
				spreadsheetName,
				[]string{payload.TicketID, payload.CustomerEmail, payload.Price.Amount, payload.Price.Currency},
			)
		},
	)
}

func processRefundsAppend(
	sub message.Subscriber,
	router *message.Router,
	action func(ctx context.Context, spreadSheetName string, row []string) error,
) {
	const spreadsheetName = "tickets-to-refund"

	router.AddNoPublisherHandler(
		"refund_handler",
		TicketBookingCanceled,
		sub,
		func(msg *message.Message) error {
			var payload PrintTicketPayload
			if err := json.Unmarshal(msg.Payload, &payload); err != nil {
				return err
			}
			return action(
				msg.Context(),
				spreadsheetName,
				[]string{payload.TicketID, payload.CustomerEmail, payload.Price.Amount, payload.Price.Currency},
			)
		},
	)
}
