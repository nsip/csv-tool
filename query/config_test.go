package query

import (
	"fmt"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/davecgh/go-spew/spew"
	lk "github.com/nsip/logkit"
)

func TestQueryConfig(t *testing.T) {
	cfg := &Config{}
	_, err := toml.DecodeFile("./query.toml", cfg)
	lk.FailOnErr("%v", err)
	fmt.Println("-------------------------------")
	spew.Dump(cfg.Query[0])
	fmt.Println("-------------------------------")
	spew.Dump(cfg.Query[1])
	fmt.Println("-------------------------------")
}
