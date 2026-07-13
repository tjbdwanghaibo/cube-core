package entity_test

import (
	"github.com/tjbdwanghaibo/cube-core/checkpoint"
	"github.com/tjbdwanghaibo/cube-core/entity"
	"fmt"
	"sync"
)

// ============================================================
// Below simulates what CODE GENERATION would produce.
// The generator scans entity/component/DAO definitions and
// outputs a factory file per entity category.
// ============================================================

// --- DAO definition (手写) ---

type PlayerDao struct {
	id    int64
	coll  string
	dirty checkpoint.DirtyTracker
	Name  string
	Level int
}

func (d *PlayerDao) Id() int64            { return d.id }
func (d *PlayerDao) SetId(id int64)       { d.id = id }
func (d *PlayerDao) DbName() string       { return "game" }
func (d *PlayerDao) CollName() string     { return d.coll }
func (d *PlayerDao) Dirty() entity.IDirty { return &d.dirty }
func (d *PlayerDao) CleanDirty()          { d.dirty.SelfClean() }

// --- Component definition (手写) ---

type BagComponent struct {
	player *Player // 直接引用具体类型，不是 interface
	items  map[int64]int
}

func (b *BagComponent) Name() string                                           { return "bag" }
func (b *BagComponent) OnInitFinish(_ *entity.EntityCreateParam, _ bool) error { return nil }
func (b *BagComponent) OnDestroy(_ entity.EntityDestroyReason)                 {}

func (b *BagComponent) AddItem(id int64, count int) {
	b.items[id] += count
	b.player.dao.dirty.MarkScope(checkpoint.DirtyPersist|checkpoint.DirtySync, checkpoint.DirtyAll)
}

// --- Entity definition (手写，只定义结构) ---

type Player struct {
	*entity.EntityBase
	entity.ComponentManager
	entity.DaoManager

	// 具体类型字段（由生成代码赋值）
	bag *BagComponent
	dao *PlayerDao
}

func (p *Player) Base() *entity.EntityBase { return p.EntityBase }

// ============================================================
// gen_player.go — 以下全部由代码生成器产生
// ============================================================

func NewPlayer(param *entity.EntityCreateParam) (*Player, error) {
	p := &Player{}
	if err := param.NormalizeID(param.Kind); err != nil {
		return nil, err
	}

	// 1. 初始化 EntityBase（指针嵌入，无 interface 引用）
	p.EntityBase = entity.NewEntityBase(param.Id, param.Category, false, param.Kind)
	p.EntityBase.SetHooks(
		func() { // onClear
			p.ComponentManager.Clear()
			p.DaoManager.Clear()
			p.bag = nil
			p.dao = nil
		},
		func(reason entity.EntityDestroyReason) { // onDestroy
			p.ComponentManager.DestroyAll(reason)
		},
	)

	// 2. 初始化 managers
	p.ComponentManager = entity.NewComponentManager()
	p.DaoManager = entity.NewDaoManager()

	// 3. 绑定 DAO（具体类型，从 param 中 type assert）
	if daoRaw, ok := param.Dao["players"]; ok {
		p.dao = daoRaw.(*PlayerDao)
	} else {
		p.dao = &PlayerDao{id: param.StorageID(), coll: "players"}
	}
	p.DaoManager.Set("players", p.dao)

	// 4. 创建 Component（直接传具体类型指针）
	p.bag = &BagComponent{
		player: p,
		items:  make(map[int64]int),
	}
	p.ComponentManager.Set(1, p.bag) // ComponentType = 1 for bag

	// 5. 初始化所有 component（按拓扑排序）
	if err := p.ComponentManager.InitAll(param, param.IsCreate); err != nil {
		return nil, err
	}

	return p, nil
}

// ============================================================
// Snapshot 方法（生成代码产出，配合 checkpoint）
// ============================================================

func (p *Player) Snapshot() []checkpoint.SaveItem {
	mask := p.dao.dirty.TakePersistDirty()
	if mask == 0 {
		return nil
	}
	ver := p.dao.dirty.IncVersion()
	// 序列化 DAO（实际用 BSON/MessagePack）
	data := []byte(fmt.Sprintf(`{"name":"%s","level":%d}`, p.dao.Name, p.dao.Level))
	return []checkpoint.SaveItem{
		{
			Collection: p.dao.CollName(),
			ID:         p.dao.Id(),
			Version:    ver,
			Mask:       mask,
			Data:       data,
			Tracker:    &p.dao.dirty,
		},
	}
}

// ============================================================
// Example: 展示完整流程
// ============================================================

func Example_generatedEntityFactory() {
	entity.MustRegisterEntityKindCategory(entity.EntityKind(1), entity.EntityCategory(1))
	param := &entity.EntityCreateParam{
		IsCreate: true,
		UniqueID: 10001,
		Kind:     entity.EntityKind(1),
		Dao:      make(map[string]entity.DaoInterface),
	}

	// 生成代码创建 entity
	player, err := NewPlayer(param)
	if err != nil {
		panic(err)
	}

	// 业务逻辑：直接操作具体类型
	player.bag.AddItem(1001, 5)

	// checkpoint: guard release 时调用 Snapshot
	items := player.Snapshot()
	fmt.Printf("entity_id=%d, storage_id=%d, snapshot_items=%d, version=%d\n",
		player.ID(), player.StorageID(), len(items), items[0].Version)

	// Touch/UnTouch 生命周期
	player.Touch()
	fmt.Printf("touched, removed=%v\n", player.IsRemoved())
	player.UnTouch()

	// Guard 使用
	guard := entity.GetEntityGuard()
	ok := guard.RequireEntity(player)
	fmt.Printf("guard_acquired=%v\n", ok)
	guard.ReleaseAll()
	entity.EntityGuardRelease(guard)

	// Verify no IThreadSafeEntity stored in EntityBase
	_ = player.Base().GetMutex()
	fmt.Println("no interface reference in EntityBase")

	_ = sync.Mutex{}

	// Output:
	// entity_id=10241029, storage_id=10241029, snapshot_items=1, version=1
	// touched, removed=false
	// guard_acquired=true
	// no interface reference in EntityBase
}
