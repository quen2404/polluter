package redis

import (
	"encoding/json"
	"github.com/romanyx/polluter/v2"

	"github.com/go-redis/redis"
	"github.com/pkg/errors"
	"github.com/romanyx/jwalk"
)

type redisEngine struct {
	cli *redis.Client
}

func (e redisEngine) Exec(cmds polluter.Commands) error {
	for _, cmd := range cmds {
		if err := e.cli.Set(cmd.Q, cmd.Args[0], 0).Err(); err != nil {
			return errors.Wrap(err, "failed to set")
		}
	}
	return nil
}

func (e redisEngine) Build(obj jwalk.ObjectWalker) (polluter.Commands, error) {
	cmds := make(polluter.Commands, 0)

	if err := obj.Walk(func(key string, value interface{}) error {
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}

		cmds = append(cmds, polluter.Command{Q: key, Args: []interface{}{data}})
		return nil
	}); err != nil {
		return nil, err
	}

	return cmds, nil
}

// RedisEngine option enables
// Redis engine for Polluter.
func RedisEngine(cli *redis.Client) polluter.DbEngine {
	return redisEngine{cli}
}
