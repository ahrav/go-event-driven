package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"

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
)

type ReceiptsClient struct {
	clients *clients.Clients
}

func NewReceiptsClient(clients *clients.Clients) ReceiptsClient {
	return ReceiptsClient{
		clients: clients,
	}
}

func (c ReceiptsClient) IssueReceipt(ctx context.Context, ticketID string) error {
	body := receipts.PutReceiptsJSONRequestBody{
		TicketId: ticketID,
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

type TicketsConfirmationRequest struct {
	Tickets []string `json:"tickets"`
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

	issueReceiptSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client:        rdb,
		ConsumerGroup: receiptsConsumerGroup,
	}, logger)
	if err != nil {
		panic(err)
	}

	appendToTrackerSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client:        rdb,
		ConsumerGroup: trackerConsumerGroup,
	}, logger)
	if err != nil {
		panic(err)
	}

	publisher, err := redisstream.NewPublisher(redisstream.PublisherConfig{Client: rdb}, logger)
	if err != nil {
		panic(err)
	}

	e := commonHTTP.NewEcho()

	c, err := clients.NewClients(os.Getenv("GATEWAY_ADDR"), nil)
	if err != nil {
		panic(err)
	}

	receiptsClient := NewReceiptsClient(c)
	spreadsheetsClient := NewSpreadsheetsClient(c)

	ctx := context.Background()
	go processIssueReceipt(ctx, issueReceiptSub, receiptsClient.IssueReceipt)
	go processTrackerAppend(ctx, appendToTrackerSub, spreadsheetsClient.AppendRow)

	e.POST("/tickets-confirmation", func(c echo.Context) error {
		var request TicketsConfirmationRequest
		err := c.Bind(&request)
		if err != nil {
			return err
		}

		for _, ticket := range request.Tickets {
			msg := message.NewMessage(watermill.NewUUID(), []byte(ticket))
			if err := publisher.Publish(issueReceiptTopic, msg); err != nil {
				return err
			}
			if err := publisher.Publish(appendToTrackerTopic, msg); err != nil {
				return err
			}
		}

		return c.NoContent(http.StatusOK)
	})

	logrus.Info("Server starting...")

	if err := e.Start(":8080"); err != nil && !errors.Is(err, http.ErrServerClosed) {
		panic(err)
	}
}

func processIssueReceipt(ctx context.Context, sub message.Subscriber, action func(ctx context.Context, ticketID string) error) {
	messages, err := sub.Subscribe(ctx, issueReceiptTopic)
	if err != nil {
		panic(err)
	}

	for msg := range messages {
		ticketID := string(msg.Payload)

		if err := action(msg.Context(), ticketID); err != nil {
			msg.Nack()
		} else {
			msg.Ack()
		}
	}
}

func processTrackerAppend(ctx context.Context, sub message.Subscriber, action func(ctx context.Context, spreadSheetName string, row []string) error) {
	const spreadsheetName = "tickets-to-print"
	messages, err := sub.Subscribe(ctx, appendToTrackerTopic)
	if err != nil {
		panic(err)
	}

	for msg := range messages {
		ticketID := string(msg.Payload)

		if err := action(msg.Context(), spreadsheetName, []string{ticketID}); err != nil {
			msg.Nack()
		} else {
			msg.Ack()
		}
	}
}
