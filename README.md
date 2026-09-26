# psql
Lightweight library for interacting with Postgres using a pure Golang implementation

## Example

```go
package main

import (
	"fmt"
	"context"
	"github.com/chuckyQ/psql"
)

func main() {

	ctx := context.TODO()

	conn, err := psql.Connect(ctx, "127.0.0.1", 5432, "postgres", "postgres", "mypassword")
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	nulls, data, columns, types, err := conn.Exec("SELECT * FROM users")

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println(columns)
	fmt.Println(types)

	for i, row := range data {
		fmt.Println(nulls[i])
		fmt.Println(row)
	}

}

```

You can also use the in-built client to have easier control of data flow

```go
package main

import (
	"context"
	"fmt"

	"github.com/chuckyQ/psql"
	"github.com/google/uuid"
)

// Add psql tags to simplify data flow
type User struct {
	ID       string   `psql:"id"`
	Username string   `psql:"username"`
	Password string   `psql:"password"`
	Tokens   []string `psql:"firebase_tokens"`
}

func NewUser(username, password string) *User {

	return &User{
		Username: username,
		Password: password,
		ID:       uuid.NewString(),
	}
}

func (u *User) Save(conn *psql.Connection) error {

	_, _, _, _, err := conn.ExecPrepared(`INSERT INTO 
			users(id, username, password, firebase_tokens) 
				   VALUES ($1, $2, $3, $4) 
				   ON CONFLICT(id) DO UPDATE 
				   SET 
				   	username = $2, 
					password = $3, 
					firebase_tokens = $4
					`,
		u.ID, u.Username, u.Password, u.Tokens)
	return err
}

func (u User) String() string {

	return fmt.Sprintf("User(id=\"%v\", username=\"%v\", password=\"%v\", tokens=%v)", u.ID, u.Username, u.Password, u.Tokens)
}

func main() {

	ctx := context.TODO()
	client, err := psql.NewClient(ctx, "127.0.0.1", 5432, "postgres", "postgres", "$s3cret$")

	var users []User
	err = client.Query(`SELECT username FROM users WHERE username = $1`, &users, "ghi")

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println(users)

}


```