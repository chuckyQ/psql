# psql
Lightweight library for interacting with Postgres using a pure Golang implementation

## Example

```go
package main

import (
	"context"
	"fmt"

	"github.com/chuckyQ/psql"
)

type User struct {
	Username string `psql:"username"`
	Password string `psql:"password"`
}

func main() {

	ctx := context.TODO()
	client, err := psql.NewClient(ctx, psql.Config{
		User:     "postgres",
		Password: "postgres",
		Database: "postgres",
	})
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	// Raw query so you can use the returned data however, you would like
	_, data, _, _, err := client.QueryRaw(ctx, `INSERT INTO users(username, password) VALUES ($1, $2)`, "alice", "bob")
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println(data)

	// Easy unmarshalling of results
	var users []User
	err = client.Query(ctx, `SELECT * FROM users`, &users)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println(users)

}
```