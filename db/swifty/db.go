package swifty

import (
	"context"
	"crypto/sha256"
	"encoding/binary"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"

	swiftyycsb "github.com/provok1ng/sp/ycsb"
)

const (
	protocol         = "swifty.protocol"
	protocolDefault  = "swift"

	server        = "swifty.server"
	serverDefault = "10.10.5.8"

	master        = "swifty.master"
	masterDefault = "10.10.5.8"

	fast        = "swifty.fast"
	fastDefault = false

	leaderless        = "swifty.leaderless"
	leaderlessDefault = false

	args        = "swifty.args"
	argsDefault = "none"
)

type contextKey string

const stateKey = contextKey("swift")

type swiftyCreator struct{}

type swiftyDB struct {
	p       *properties.Properties
	clients []swiftyycsb.SwiftClient
}

func init() {
	ycsb.RegisterDBCreator("swifty", swiftyCreator{})
}

func (c swiftyCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	return &swiftyDB{
		p: p,
		clients: nil,
	}, nil

	// t := p.GetString(protocol, protocolDefault)
	// s := p.GetString(server, serverDefault)
	// f := p.GetBool(fast, fastDefault)
	// l := p.GetBool(leaderless, leaderlessDefault)
	// a := p.GetString(args, argsDefault)

	// sc := swiftyycsb.NewswiftyClient(t, s, f, l, a)
	// if sc == nil {
	// 	return nil, nil
	// }
	// //err := sc.Connect()

	// return &swiftyDB{
	// 	p: p,
	// 	client: sc,
	// }, nil
}

func (db *swiftyDB) InitThread(ctx context.Context, _, _ int) context.Context {
	t := db.p.GetString(protocol, protocolDefault)
	s := db.p.GetString(server, serverDefault)
	f := db.p.GetBool(fast, fastDefault)
	l := db.p.GetBool(leaderless, leaderlessDefault)
	a := db.p.GetString(args, argsDefault)
	m := db.p.GetString(master, masterDefault)

	sc := swiftyycsb.NewswiftClient(t, s, m, 7087, f, l, a)
	if sc == nil {
		return ctx
	}
	db.clients = append(db.clients, sc)
	return context.WithValue(ctx, stateKey, sc)
}

func (db *swiftyDB) CleanupThread(_ context.Context) {
}

func (db *swiftyDB) Close() error {
	for _, c := range db.clients {
		c.Disconnect()
	}
	return nil
}

func (db *swiftyDB) Read(ctx context.Context, table string, key string, _ []string) (map[string][]byte, error) {
	client := ctx.Value(stateKey).(swiftyycsb.SwiftClient)

	ks := sha256.Sum256([]byte(table+":"+key))
	kks := make([]byte, 32)
	for i, k := range ks {
		kks[i] = k
	}
	k := binary.BigEndian.Uint64(kks)
	client.Read(int64(k))
	return nil, nil
}

func (db *swiftyDB) Scan(ctx context.Context, table string, startKey string, count int, _ []string) ([]map[string][]byte, error) {
	client := ctx.Value(stateKey).(swiftyycsb.SwiftClient)

	ks := sha256.Sum256([]byte(table+":"+startKey))
	kks := make([]byte, 32)
	for i, k := range ks {
		kks[i] = k
	}
	k := binary.BigEndian.Uint64(kks)
	client.Scan(int64(k), int64(count))
	return nil, nil
}

func (db *swiftyDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	client := ctx.Value(stateKey).(swiftyycsb.SwiftClient)

	ks := sha256.Sum256([]byte(table+":"+key))
	kks := make([]byte, 32)
	for i, k := range ks {
		kks[i] = k
	}
	k := binary.BigEndian.Uint64(kks)
	for _, v := range values {
		client.Write(int64(k), v)
		break
	}
	return nil
}

func (db *swiftyDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	return db.Update(ctx, table, key, values)
}

func (db *swiftyDB) Delete(context.Context, string, string) error {
	return nil
}
