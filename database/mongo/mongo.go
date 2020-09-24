package mongo

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/romanyx/jwalk"
	"github.com/romanyx/polluter/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type mongoEngine struct {
	db *mongo.Database
}

func (m mongoEngine) Exec(cmds polluter.Commands) error {
	for _, c := range cmds {
		coll := m.db.Collection(c.Q)
		if _, err := coll.InsertMany(context.Background(), c.Args); err != nil {
			return errors.Wrap(err, "failed to insert one")
		}
	}
	return nil
}

func (m mongoEngine) Build(obj jwalk.ObjectWalker) (polluter.Commands, error) {
	cmds := make(polluter.Commands, 0)
	if err := obj.Walk(func(collection string, value interface{}) error {
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		docs := make([]bson.D, 0)
		if err = bson.UnmarshalExtJSON(data, true, &docs); err != nil {
			return err
		}
		args := make([]interface{}, len(docs))
		for i, doc := range docs {
			args[i] = doc
		}
		cmds = append(cmds, polluter.Command{Q: collection, Args: args})
		return nil
	}); err != nil {
		return nil, err
	}

	return cmds, nil
}

// MongoEngine option enables
// Mongo engine for Polluter.
func MongoEngine(cli *mongo.Database) polluter.DbEngine {
	return mongoEngine{cli}
}
