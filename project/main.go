package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"

	"github.com/ThreeDotsLabs/go-event-driven/common/clients"
	"github.com/ThreeDotsLabs/go-event-driven/common/clients/receipts"
	"github.com/ThreeDotsLabs/go-event-driven/common/clients/spreadsheets"
	commonHTTP "github.com/ThreeDotsLabs/go-event-driven/common/http"
	"github.com/ThreeDotsLabs/go-event-driven/common/log"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type ReceiptsClient struct {
	clients *clients.Clients
}

func NewReceiptsClient(clients *clients.Clients) ReceiptsClient {
	return ReceiptsClient{
		clients: clients,
	}
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

type SpreadsheetsClient struct {
	clients *clients.Clients
}

func NewSpreadsheetsClient(clients *clients.Clients) SpreadsheetsClient {
	return SpreadsheetsClient{
		clients: clients,
	}
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

type Ticket struct {
	TicketId      string `json:"ticket_id"`
	Status        string `json:"status"`
	CustomerEmail string `json:"customer_email"`
	Price         Money  `json:"price"`
}

type TicketsConfirmationRequest struct {
	Tickets []Ticket `json:"tickets"`
}

const (
	// Topics.
	issueReceiptTopic    = "issue-receipt"
	appendToTrackerTopic = "append-to-tracker"

	// Consumer groups.
	receiptsConsumerGroup = "receipts"
	trackerConsumerGroup  = "tracker"
)

func main() {
	log.Init(logrus.InfoLevel)

	logger := log.NewWatermill(logrus.NewEntry(logrus.StandardLogger()))

	rdb := redis.NewClient(&redis.Options{Addr: os.Getenv("REDIS_ADDR")})

	const (
		issueReceiptTopic    = "issue-receipt"
		appendToTrackerTopic = "append-to-tracker"
	)

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

	spreadsheetsClient := NewSpreadsheetsClient(c)
	workerPool.Go(func() error {
		processTrackerAppend(appendToTrackerSub, router, spreadsheetsClient.AppendRow)
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
			receiptReq, err := json.Marshal(IssueReceiptRequest{
				TicketID: ticket.TicketId,
				Price:    ticket.Price,
			})
			if err != nil {
				logger.Error("error marshalling IssueReceiptRequest", err, watermill.LogFields{"ticket": ticket})
				continue
			}

			printReq, err := json.Marshal(PrintTicketPayload{
				TicketID:      ticket.TicketId,
				CustomerEmail: ticket.CustomerEmail,
				Price:         ticket.Price,
			})
			if err != nil {
				logger.Error("error marshalling PrintTicketPayload", err, watermill.LogFields{"ticket": ticket})
				continue
			}

			if err := publisher.Publish(issueReceiptTopic, message.NewMessage(watermill.NewUUID(), receiptReq)); err != nil {
				return err
			}
			if err := publisher.Publish(appendToTrackerTopic, message.NewMessage(watermill.NewUUID(), printReq)); err != nil {
				return err
			}
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

func processIssueReceipt(sub message.Subscriber, router *message.Router, action func(ctx context.Context, request IssueReceiptRequest) error) {
	router.AddNoPublisherHandler(
		"issue_receipt_handler",
		issueReceiptTopic,
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

func processTrackerAppend(sub message.Subscriber, router *message.Router, action func(ctx context.Context, spreadSheetName string, row []string) error) {
	const spreadsheetName = "tickets-to-print"

	router.AddNoPublisherHandler(
		"tracker_append_handler",
		appendToTrackerTopic,
		sub,
		func(msg *message.Message) error {
			var payload PrintTicketPayload
			if err := json.Unmarshal(msg.Payload, &payload); err != nil {
				return err
			}
			return action(msg.Context(), spreadsheetName, []string{payload.TicketID, payload.CustomerEmail, payload.Price.Amount, payload.Price.Currency})
		},
	)
}
