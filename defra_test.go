package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/net"
	net_pb "github.com/sourcenetwork/defradb/net/pb"
	netutils "github.com/sourcenetwork/defradb/net/utils"
	tests "github.com/sourcenetwork/defradb/tests/integration"
	"github.com/stretchr/testify/require"
)

type State struct {
	Nodes       []*net.Node
	Collections map[int][]client.Collection
	DBS         []client.DB
}

func (s *State) UpdateCollections(t *testing.T) {
	for idx, node := range s.Nodes {
		cols, err := node.DB.GetAllCollections(context.Background())
		require.NoError(t, err)
		s.Collections[idx] = cols
	}
}

func (s *State) SubscribeToAllCollection(t *testing.T) {
	for i, cols := range s.Collections {
		ids := []string{}
		for _, col := range cols {
			ids = append(ids, col.SchemaID())
		}
		_, err := s.Nodes[i].Peer.AddP2PCollections(context.TODO(), &net_pb.AddP2PCollectionsRequest{
			Collections: ids,
		})
		require.NoError(t, err)
	}
}

func InsertPair(t *testing.T, col client.Collection, key, value string) {
	doc, err := client.NewDocFromJSON([]byte(fmt.Sprintf(`{"Key":"%s", "Value":"%s"}`, key, value)))
	require.NoError(t, err)
	err = col.Save(context.Background(), doc)
	require.NoError(t, err)
}

func WaitForSync(state *State, last string) {
	// all state should have last key
outer:
	for {
		for _, db := range state.DBS {
			res := db.ExecRequest(context.TODO(), fmt.Sprintf(`
			query {
				Pair(filter: {Key:{_eq:"%s"}}){
					Value
				}
			}
			`, last))

			val, ok := res.GQL.Data.([]map[string]any)

			if !ok {
				continue outer
			}
			if len(val) == 0 {
				continue outer
			}
			_, ok = val[0]["Value"]
			if !ok {
				continue outer
			}
		}
		break
	}
}

func AssertPair(t *testing.T, db client.DB, key string) {
	res := db.ExecRequest(context.TODO(), fmt.Sprintf(`
	query {
		Pair(filter: {Key:{_eq:"%s"}}){
			Value
		}
	}
	`, key))
	require.Equal(t, len(res.GQL.Errors), 0, fmt.Sprintf("counld not find key %s", key))
	val, ok := res.GQL.Data.([]map[string]any)
	require.True(t, ok, fmt.Sprintf("counld not find key %s", key))
	require.Equal(t, len(val), 1, fmt.Sprintf("counld not find key %s", key))
	result, ok := val[0]["Value"]
	require.True(t, ok, fmt.Sprintf("counld not find key %s", key))
	conv, ok := result.(string)
	require.True(t, ok, fmt.Sprintf("counld not find key %s", key))
	require.Equal(t, conv, key)
}

func createDefraNodes(t *testing.T, schema string, n int) *State {
	state := &State{
		Nodes:       make([]*net.Node, 0),
		Collections: make(map[int][]client.Collection),
		DBS:         make([]client.DB, 0),
	}
	for i := 0; i < n; i++ {
		dbTypes := tests.GetDatabaseTypes()
		cfg := tests.RandomNetworkingConfig()()
		fmt.Println(dbTypes)
		db, path, err := tests.GetDatabase(context.Background(), t, dbTypes[0])
		state.DBS = append(state.DBS, db)
		require.NoError(t, err)
		fmt.Println(path)
		cfg.Datastore.Badger.Path = fmt.Sprintf("./node%d", i)
		node, err := net.NewNode(context.Background(), db, net.WithConfig(&cfg))
		require.NoError(t, err)
		node.Start()
		state.Nodes = append(state.Nodes, node)
	}
	//	connect nodes with each other.
	for x, node := range state.Nodes {
		addrs := []string{}
		for y, node := range state.Nodes {
			if x == y {
				continue
			}
			address := fmt.Sprintf("%s/p2p/%s", node.ListenAddrs()[0].String(), node.PeerID())
			addrs = append(addrs, address)
		}
		infos, err := netutils.ParsePeers(addrs)
		require.NoError(t, err)
		node.Boostrap(infos)
	}

	for _, node := range state.Nodes {
		_, err := node.DB.AddSchema(context.TODO(), schema)
		require.NoError(t, err)
	}
	return state
}

func TestDefraDB(t *testing.T) {
	state := createDefraNodes(t, `
	type Pair {
		Key: String @primary
		Value: String
	}
	`, 3)
	state.UpdateCollections(t)
	state.SubscribeToAllCollection(t)

	start := time.Now()
	// let's add pair to one node
	for i := 0; i < 5000; i++ {
		InsertPair(t, state.Collections[0][0], fmt.Sprintf("%d", i), fmt.Sprintf("%d", i))
	}

	WaitForSync(state, "9")
	fmt.Println("time taken", time.Since(start))
	for _, db := range state.DBS {
		for i := 0; i < 10; i++ {
			AssertPair(t, db, fmt.Sprintf("%d", i))
		}
	}
}

func TestDefraMultipleWriter(t *testing.T) {
	state := createDefraNodes(t, `
	type Pair {
		Key: String @primary
		Value: String
	}
	`, 3)
	state.UpdateCollections(t)
	state.SubscribeToAllCollection(t)
	entirePerCollection := 5000
	start := time.Now()
	wg := new(sync.WaitGroup)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(col client.Collection, idx int) {
			wg.Done()
			start := idx * entirePerCollection
			end := (idx + 1) * entirePerCollection
			for ; start <= end; start++ {
				InsertPair(t, col, fmt.Sprintf("%d", start), fmt.Sprintf("%d", start))
			}
		}(state.Collections[i][0], i)
	}
	wg.Wait()
	syncWait := time.Now()
	WaitForSync(state, fmt.Sprintf("%d", entirePerCollection*3))
	fmt.Println("time taken", time.Since(start))
	fmt.Println("sync wait", time.Since(syncWait))
	for _, db := range state.DBS {
		for i := 0; i < 3*entirePerCollection; i++ {
			AssertPair(t, db, fmt.Sprintf("%d", i))
		}
	}
}
