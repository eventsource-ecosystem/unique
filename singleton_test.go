package unique_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/eventsource-ecosystem/eventsource"
	"github.com/eventsource-ecosystem/unique"
)

func getAPI(t *testing.T) *dynamodb.DynamoDB {
	endpoint := os.Getenv("DYNAMODB_ENDPOINT")
	if endpoint == "" {
		t.SkipNow()
	}

	s := session.Must(session.NewSession(aws.NewConfig().
		WithRegion("us-west-2").
		WithEndpoint(endpoint).
		WithCredentials(credentials.NewStaticCredentials("blah", "blah", "blah"))))

	return dynamodb.New(s)
}

func TestRegistry_Lifecycle(t *testing.T) {
	api := getAPI(t)

	ctx := context.Background()
	resource := unique.Resource{
		Type:  "email",
		ID:    "id",
		Owner: "abc",
	}
	other := unique.Resource{
		Type:  resource.Type,
		ID:    resource.ID,
		Owner: resource.Owner + "blah",
	}

	TempTable(t, api, func(tableName string) {
		registry, err := unique.New(tableName,
			unique.WithDynamoDB(api),
		)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		// Should be available, no one's allocated it
		err = registry.IsAvailable(ctx, resource)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		// Reserve it
		err = registry.Reserve(ctx, resource, time.Hour)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		// Owner should show it as available
		err = registry.IsAvailable(ctx, resource)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		// But others will see it as occupied
		err = registry.IsAvailable(ctx, other)
		if err == nil {
			t.Fatalf("got nil; want not nil")
		}

		// However, once we release it
		err = registry.Release(ctx, resource)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		// Others may see it as available
		err = registry.IsAvailable(ctx, other)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})
}

func TestRegistry_ReleaseIdempotent(t *testing.T) {
	api := getAPI(t)

	ctx := context.Background()
	resource := unique.Resource{
		Type:  "email",
		ID:    "id",
		Owner: "abc",
	}

	TempTable(t, api, func(tableName string) {
		registry, err := unique.New(tableName,
			unique.WithDynamoDB(api),
		)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		// Reserve it
		err = registry.Reserve(ctx, resource, time.Hour)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		// However, once we release it
		err = registry.Release(ctx, resource)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		// However, once we release it
		err = registry.Release(ctx, resource)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})
}

func TestRegistry_AllocateIdempotent(t *testing.T) {
	api := getAPI(t)

	ctx := context.Background()
	resource := unique.Resource{
		Type:  "email",
		ID:    "id",
		Owner: "abc",
	}

	TempTable(t, api, func(tableName string) {
		registry, err := unique.New(tableName,
			unique.WithDynamoDB(api),
		)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		// Reserve it
		err = registry.Reserve(ctx, resource, time.Hour)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		// Reserve it
		err = registry.Reserve(ctx, resource, time.Hour)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})
}

func TestRegistry_Wrap(t *testing.T) {
	api := getAPI(t)

	ctx := context.Background()
	resource := unique.Resource{
		Type:  "email",
		ID:    "id",
		Owner: "user-1",
	}

	TempTable(t, api, func(tableName string) {
		registry, err := unique.New(tableName,
			unique.WithDynamoDB(api),
		)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		// user-1 allocates it
		err = registry.Reserve(ctx, resource, time.Hour)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		fn := unique.RepositoryFunc(func(ctx context.Context, command eventsource.Command) (int, error) {
			return 0, nil
		})
		repo := registry.Wrap(fn)

		// the original allocator can dispatch the command
		_, err = repo.Apply(ctx, Command{
			ID:    resource.ID,
			Owner: resource.Owner,
		})
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		// but another user cannot
		_, err = repo.Apply(ctx, Command{
			ID:    resource.ID,
			Owner: resource.Owner + "blah",
		})
		if err == nil {
			t.Fatalf("got nil; want not nil")
		}
		if got, want := unique.IsAlreadyReserved(err), true; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	})
}

type Command struct {
	eventsource.CommandModel
	ID    string
	Owner string
}

func (c Command) Reserve() (unique.Resource, time.Duration) {
	return unique.Resource{
		Type:  "email",
		ID:    c.ID,
		Owner: c.Owner,
	}, time.Hour
}
