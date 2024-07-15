package swiftpaxos

import (
	"fmt"
	"context"
	"crypto/sha256"
	"encoding/binary"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"

	swiftycsb "github.com/provok1ng/sp/ycsb"
)

const (
	protocol         = "swiftpaxos.protocol"
	protocolDefault  = "swiftpaxos"

	server        = "swiftpaxos.server"
	serverDefault = "172.17.0.5"

	master        = "swiftpaxos.master"
	masterDefault = "172.17.0.4"

	fast        = "swiftpaxos.fast"
	fastDefault = false

	leaderless        = "swiftpaxos.leaderless"
	leaderlessDefault = false

	args        = "swiftpaxos.args"
	argsDefault = "none"
)

type contextKey string

const stateKey = contextKey("swiftpaxos")

type swiftCreator struct{}

type swiftDB struct {
	p       *properties.Properties
	clients []swiftycsb.SwiftClient
}

func init() {
	ycsb.RegisterDBCreator("swiftpaxos", swiftCreator{})
}

func (c swiftCreator) Create(p *properties.Properties) (ycsb.DB, error) {
    // 取消注释并修复以下代码
    t := p.GetString(protocol, protocolDefault)
    s := p.GetString(server, serverDefault)
    f := p.GetBool(fast, fastDefault)
    l := p.GetBool(leaderless, leaderlessDefault)
    a := p.GetString(args, argsDefault)
    m := p.GetString(master, masterDefault)
    // 确保 NewswiftClient 能够正确创建并返回一个 swiftClient 实例
    sc := swiftycsb.NewswiftClient(t, s, m, 7087, f, l, a)
    if sc == nil {
    return nil, fmt.Errorf("failed to create swift client with parameters: protocol=%s, server=%s, master=%s, fast=%v, leaderless=%v, args=%s", t, s, m, f, l, a)
    }
    if sc == nil {
        return nil, fmt.Errorf("failed to create swift client")
    }
    // 确保 client 被正确赋值
    return &swiftDB{
        p:   p,
        clients: []swiftycsb.SwiftClient{sc}, // 注意：这里假设 NewswiftClient 返回 *swiftycsb.swiftClient 类型
    }, nil
}

func (db *swiftDB) InitThread(ctx context.Context, _, _ int) context.Context {
    client := db.clients[0]
    // 以下代码使用 client 来初始化 context
    // ...
    return context.WithValue(ctx, stateKey, client)
}

func (db *swiftDB) CleanupThread(_ context.Context) {
}

func (db *swiftDB) Close() error {
	for _, c := range db.clients {
		c.Disconnect()
	}
	return nil
}

func (db *swiftDB) Read(ctx context.Context, table string, key string, _ []string) (map[string][]byte, error) {
	client := ctx.Value(stateKey).(swiftycsb.SwiftClient)

	ks := sha256.Sum256([]byte(table+":"+key))
	kks := make([]byte, 32)
	for i, k := range ks {
		kks[i] = k
	}
	k := binary.BigEndian.Uint64(kks)
	client.Read(int64(k))
	return nil, nil
}

func (db *swiftDB) Scan(ctx context.Context, table string, startKey string, count int, _ []string) ([]map[string][]byte, error) {
	client := ctx.Value(stateKey).(swiftycsb.SwiftClient)

	ks := sha256.Sum256([]byte(table+":"+startKey))
	kks := make([]byte, 32)
	for i, k := range ks {
		kks[i] = k
	}
	k := binary.BigEndian.Uint64(kks)
	client.Scan(int64(k), int64(count))
	return nil, nil
}

func (db *swiftDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	client := ctx.Value(stateKey).(swiftycsb.SwiftClient)

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

func (db *swiftDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	return db.Update(ctx, table, key, values)
}

func (db *swiftDB) Delete(context.Context, string, string) error {
	return nil
}
