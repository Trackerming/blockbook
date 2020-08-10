package bchain

import (
	"math/big"
	"time"

	"github.com/golang/glog"
)

type chanInputPayload struct {
	tx    *MempoolTx
	index int
}

// MempoolBitcoinType is mempool handle.
type MempoolBitcoinType struct {
	BaseMempool
	chanTxid            chan string
	chanAddrIndex       chan txidio
	AddrDescForOutpoint AddrDescForOutpointFunc
}

// NewMempoolBitcoinType creates new mempool handler.
// For now there is no cleanup of sync routines, the expectation is that the mempool is created only once per process
func NewMempoolBitcoinType(chain BlockChain, workers int, subworkers int) *MempoolBitcoinType {
	m := &MempoolBitcoinType{
		BaseMempool: BaseMempool{
			chain:        chain,
			txEntries:    make(map[string]txEntry),
			addrDescToTx: make(map[string][]Outpoint),
		},
		chanTxid:      make(chan string, 1),
		chanAddrIndex: make(chan txidio, 1),
	}
	for i := 0; i < workers; i++ {
		// 根据worker数量创建同步mempool的工作协程
		go func(i int) {
			chanInput := make(chan chanInputPayload, 1)
			chanResult := make(chan *addrIndex, 1)
			// 创建子协程
			for j := 0; j < subworkers; j++ {
				go func(j int) {
					for payload := range chanInput {
						ai := m.getInputAddress(&payload)
						chanResult <- ai // 将获取到的vin的原始utxo结构描述写入channel
					}
				}(j)
			}
			// 猜测m.chanTxid是属于zmq消息写入的一个触发同步消息，reSync发现有不一致的txid也会写入这个channel，这个worker就是专门同步这笔交易的处理
			for txid := range m.chanTxid {
				io, ok := m.getTxAddrs(txid, chanInput, chanResult)
				if !ok {
					io = []addrIndex{}
				}
				// 这笔交易的addrIndex都写入channel，包括input和output
				m.chanAddrIndex <- txidio{txid, io}
			}
		}(i)
	}
	glog.Info("mempool: starting with ", workers, "*", subworkers, " sync workers")
	return m
}

// 主要功能是获取mempool当中的交易的输入数据（即所用utxo的锁定脚本和数值）
func (m *MempoolBitcoinType) getInputAddress(payload *chanInputPayload) *addrIndex {
	var addrDesc AddressDescriptor
	var value *big.Int
	// chanInputPayload的index应该是对应交易输入的index
	vin := &payload.tx.Vin[payload.index]
	// TODO：AddrDescForOutpointFunc初始化位置？
	if m.AddrDescForOutpoint != nil {
		addrDesc, value = m.AddrDescForOutpoint(Outpoint{vin.Txid, int32(vin.Vout)})
	}
	if addrDesc == nil {
		// 从链上获取数据去核对
		itx, err := m.chain.GetTransactionForMempool(vin.Txid)
		if err != nil {
			glog.Error("cannot get transaction ", vin.Txid, ": ", err)
			return nil
		}
		// 判断vin是合法的
		if int(vin.Vout) >= len(itx.Vout) {
			glog.Error("Vout len in transaction ", vin.Txid, " ", len(itx.Vout), " input.Vout=", vin.Vout)
			return nil
		}
		// 返回的锁定脚本的byte数组
		addrDesc, err = m.chain.GetChainParser().GetAddrDescFromVout(&itx.Vout[vin.Vout])
		if err != nil {
			glog.Error("error in addrDesc in ", vin.Txid, " ", vin.Vout, ": ", err)
			return nil
		}
		value = &itx.Vout[vin.Vout].ValueSat
	}
	vin.AddrDesc = addrDesc
	vin.ValueSat = *value
	return &addrIndex{string(addrDesc), ^int32(vin.Vout)}

}

func (m *MempoolBitcoinType) getTxAddrs(txid string, chanInput chan chanInputPayload, chanResult chan *addrIndex) ([]addrIndex, bool) {
	// 获取交易从链上
	tx, err := m.chain.GetTransactionForMempool(txid)
	if err != nil {
		glog.Error("cannot get transaction ", txid, ": ", err)
		return nil, false
	}
	glog.V(2).Info("mempool: gettxaddrs ", txid, ", ", len(tx.Vin), " inputs")
	// 转换获取到的交易格式为mempool处理的格式
	mtx := m.txToMempoolTx(tx)
	// 交易的io次数为输入与输出个数的和的addrIndex结构
	io := make([]addrIndex, 0, len(tx.Vout)+len(tx.Vin))
	for _, output := range tx.Vout {
		addrDesc, err := m.chain.GetChainParser().GetAddrDescFromVout(&output)
		if err != nil {
			glog.Error("error in addrDesc in ", txid, " ", output.N, ": ", err)
			continue
		}
		if len(addrDesc) > 0 {
			io = append(io, addrIndex{string(addrDesc), int32(output.N)})
		}
		if m.OnNewTxAddr != nil {
			m.OnNewTxAddr(tx, addrDesc)
		}
	}
	dispatched := 0
	for i := range tx.Vin {
		input := &tx.Vin[i]
		if input.Coinbase != "" {
			continue
		}
		payload := chanInputPayload{mtx, i}
	loop:
		for {
			select {
			// store as many processed results as possible
			case ai := <-chanResult:
				if ai != nil {
					io = append(io, *ai)
				}
				dispatched--
			// send input to be processed
			// 写入消息触发子协程去同步input获取结果
			case chanInput <- payload:
				dispatched++
				break loop
			}
		}
	}
	// TODO：为什么两个地方去读取chanResult并写入？担心select读不完整？
	for i := 0; i < dispatched; i++ {
		ai := <-chanResult
		if ai != nil {
			io = append(io, *ai)
		}
	}
	if m.OnNewTx != nil {
		m.OnNewTx(mtx)
	}
	return io, true
}

// Resync gets mempool transactions and maps outputs to transactions.
// Resync is not reentrant, it should be called from a single thread.
// Read operations (GetTransactions) are safe.
func (m *MempoolBitcoinType) Resync() (int, error) {
	start := time.Now()
	glog.V(1).Info("mempool: resync")
	// 获取当前链上mempool中的待确认交易的hash数组
	txs, err := m.chain.GetMempoolTransactions()
	if err != nil {
		return 0, err
	}
	glog.V(2).Info("mempool: resync ", len(txs), " txs")
	onNewEntry := func(txid string, entry txEntry) {
		// 若entry有数据给mempool加锁去加入新的tx数据
		if len(entry.addrIndexes) > 0 {
			m.mux.Lock()
			m.txEntries[txid] = entry
			for _, si := range entry.addrIndexes {
				m.addrDescToTx[si.addrDesc] = append(m.addrDescToTx[si.addrDesc], Outpoint{txid, si.n})
			}
			m.mux.Unlock()
		}
	}
	txsMap := make(map[string]struct{}, len(txs))
	dispatched := 0
	txTime := uint32(time.Now().Unix())
	// get transaction in parallel using goroutines created in NewUTXOMempool
	for _, txid := range txs {
		txsMap[txid] = struct{}{}
		// 若txEntry中不存在则添加进mempool
		_, exists := m.txEntries[txid]
		if !exists {
		loop:
			for {
				select {
				// store as many processed transactions as possible
				case tio := <-m.chanAddrIndex:
					// 处理结果写入txEntry，包括获取到的时间
					onNewEntry(tio.txid, txEntry{tio.io, txTime})
					dispatched--
				// send transaction to be processed
				case m.chanTxid <- txid:
					dispatched++
					break loop
				}
			}
		}
	}
	for i := 0; i < dispatched; i++ {
		tio := <-m.chanAddrIndex
		onNewEntry(tio.txid, txEntry{tio.io, txTime})
	}

	// 同时需要去删除链上已经不存在的entry保持和链上一致
	for txid, entry := range m.txEntries {
		if _, exists := txsMap[txid]; !exists {
			m.mux.Lock()
			m.removeEntryFromMempool(txid, entry)
			m.mux.Unlock()
		}
	}
	glog.Info("mempool: resync finished in ", time.Since(start), ", ", len(m.txEntries), " transactions in mempool")
	return len(m.txEntries), nil
}
