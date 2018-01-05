// Copyright 2017 Xiaomi, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"fmt"
	"log"

	tlist "github.com/toolkits/container/list"
	"github.com/toolkits/container/nmap"
	ttime "github.com/toolkits/time"

	"github.com/open-falcon/falcon-plus/modules/nodata/g"
)

// 主动收集到的监控数据 的缓存
var (
	// map - list
	ItemMap = nmap.NewSafeMap()
)

func Start() {
	if !g.Config().Collector.Enabled {
		log.Println("collector.Start warning, not enabled")
		return
	}

	StartCollectorCron()
	log.Println("collector.Start ok")
}

// Interfaces Of ItemMap
func GetFirstItem(key string) (*DataItem, bool) {
	listv, found := ItemMap.Get(key)
	if !found || listv == nil {
		return &DataItem{}, false
	}

	first := listv.(*tlist.SafeListLimited).Front()
	if first == nil {
		return &DataItem{}, false
	}

	return first.(*DataItem), true
}

//return leftTsItem, tsItem, rightTsItem
func GetItemByKeyAndTs(key string, ts int64) (*DataItem, *DataItem, *DataItem) {
	listv, found := ItemMap.Get(key)
	if !found || listv == nil {
		return nil, nil, nil
	}

	all := listv.(*tlist.SafeListLimited).FrontAll()
	if all == nil || len(all) == 0 {
		return nil, nil, nil
	}
	if g.Config().Debug {
		log.Printf("getItemByIndex key %s, ts %d, list %v\n", key, ts, all)
	}
	var leftTsItem, tsItem, rightTsItem *DataItem
	for _, item := range all {
		itemTs := item.(*DataItem).Ts
		if itemTs > ts {
			if rightTsItem == nil || rightTsItem.Ts > itemTs {
				rightTsItem = item.(*DataItem)
			}
		} else if itemTs == ts {
			tsItem = item.(*DataItem)
		} else {
			if leftTsItem == nil || leftTsItem.Ts < itemTs {
				leftTsItem = item.(*DataItem)
			}
		}
	}
	return leftTsItem, tsItem, rightTsItem
}

func AddItem(key string, val *DataItem) {
	listv, found := ItemMap.Get(key)
	if !found {
		ll := tlist.NewSafeListLimited(12) //每个采集指标,缓存最新的3个数据点，比10多2个点防止取不到after和before
		ll.PushFrontViolently(val)
		ItemMap.Put(key, ll)
		return
	}
	all := listv.(*tlist.SafeListLimited).FrontAll()
	minTs := val.Ts
	for _, item := range all {
		itemTs := item.(*DataItem).Ts
		if minTs > itemTs {
			minTs = itemTs
		}
		if itemTs == val.Ts {//已经有值了，不用存储
			return
		}
	}

	if len(all) < 12 {//不够12个，直接存
		listv.(*tlist.SafeListLimited).PushFrontViolently(val)
	}
	if val.Ts > minTs {//TODO 有可能push的时候，把不是minTs的数据pop出去
		//不进行重复写，后续sort后，进行补数据
		listv.(*tlist.SafeListLimited).PushFrontViolently(val)
	}
}

func RemoveItem(key string) {
	ItemMap.Remove(key)
}

// NoData Data Item Struct
type DataItem struct {
	Ts      int64
	Value   float64
	FStatus string // OK|ERR
	FTs     int64
}

func NewDataItem(ts int64, val float64, fstatus string, fts int64) *DataItem {
	return &DataItem{Ts: ts, Value: val, FStatus: fstatus, FTs: fts}
}

func (this *DataItem) String() string {
	return fmt.Sprintf("ts:%s, value:%f, fts:%s, fstatus:%s",
		ttime.FormatTs(this.Ts), this.Value, ttime.FormatTs(this.FTs), this.FStatus)
}
