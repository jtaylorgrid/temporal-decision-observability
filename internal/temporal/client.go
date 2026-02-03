package temporal

import (
	"context"

	"go.temporal.io/sdk/client"
)

type Client struct {
	sdk       client.Client
	namespace string
}

func NewClient(hostPort, namespace string) (*Client, error) {
	c, err := client.Dial(client.Options{
		HostPort:  hostPort,
		Namespace: namespace,
	})
	if err != nil {
		return nil, err
	}

	return &Client{
		sdk:       c,
		namespace: namespace,
	}, nil
}

func (c *Client) SDK() client.Client {
	return c.sdk
}

func (c *Client) Namespace() string {
	return c.namespace
}

func (c *Client) Close() {
	c.sdk.Close()
}

func (c *Client) CheckHealth(ctx context.Context) error {
	_, err := c.sdk.CheckHealth(ctx, nil)
	return err
}
