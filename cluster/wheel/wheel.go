package cluster

import (
	"container/list"
	"sync"
	"time"
)

const (
	DefaultSlotNum = 10
	DefaultInterval
)

// 定时任务信息
type taskElement struct {
	task  func() // 定时任务逻辑
	pos   int    // 定时任务在数组中的索引位置
	cycle int    // 定时任务延迟轮次
	key   string // 唯一标识定时任务
}

// TimeWheel 单机时间轮
type TimeWheel struct {
	sync.Once                             // 单例
	interval     time.Duration            // 时间轮运行时间间隔
	ticker       *time.Ticker             // 时间轮定时器
	stopCh       chan struct{}            // 停止时间轮的通道
	addTaskCh    chan *taskElement        // 新增定时任务的入口
	removeTaskCh chan string              // 删除定时任务的入口
	slots        []*list.List             // 定时任务数量较大 每个slot
	curSlot      int                      // 当前遍历到的环状数组的索引
	keyToETask   map[string]*list.Element // 定时任务key到任务节点的映射 便于在list中删除
}

// NewTimeWheel 创建时间轮
func NewTimeWheel(slotNum int, interval time.Duration) *TimeWheel {
	// 环状数组默认长度
	if slotNum <= 0 {
		slotNum = DefaultSlotNum
	}
	if interval <= 0 {
		interval = DefaultInterval
	}

	// 初始化时间轮
	t := TimeWheel{
		interval:     interval,
		ticker:       time.NewTicker(interval),
		stopCh:       make(chan struct{}),
		keyToETask:   make(map[string]*list.Element),
		slots:        make([]*list.List, 0, slotNum),
		addTaskCh:    make(chan *taskElement),
		removeTaskCh: make(chan string),
	}

	for i := 0; i < slotNum; i++ {
		t.slots = append(t.slots, list.New())
	}

	// 异步启动时间轮
	go t.run()

	return &t
}

func (t *TimeWheel) run() {
	defer func() {
		if err := recover(); err != nil {
			// ...
		}
	}()

	for {
		select {
		case <-t.stopCh:
			return
		case <-t.ticker.C:
			// 接收定时任务
			t.tick()
		case task := <-t.addTaskCh:
			// 创建定时任务的信号
			t.addTask(task)

		case removeKey := <-t.removeTaskCh:
			// 删除定时任务
			t.removeTask(removeKey)
		}
	}
}

// AddTask 添加任务
func (t *TimeWheel) AddTask(key string, task func(), executeAt time.Time) {
	pos, cycle := t.getPosAndCircle(executeAt)

	// 投递定时任务
	t.addTaskCh <- &taskElement{
		pos:   pos,
		cycle: cycle,
		task:  task,
		key:   key,
	}
}

func (t *TimeWheel) addTask(task *taskElement) {
	taskList := t.slots[task.pos]
	// 如果定时任务key之前存在 则先删除定时任务
	if _, ok := t.keyToETask[task.key]; ok {
		t.removeTask(task.key)
	}

	// 将定时任务追加到list尾部
	et := taskList.PushBack(task)
	t.keyToETask[task.key] = et
}

func (t *TimeWheel) RemoveTask(key string) {
	t.removeTaskCh <- key
}

func (t *TimeWheel) removeTask(key string) {
	et, ok := t.keyToETask[key]
	if !ok {
		return
	}

	// 将定时任务节点从映射中移除
	delete(t.keyToETask, key)
	// 获取到定时任务节点
	task, _ := et.Value.(*taskElement)
	_ = t.slots[task.pos].Remove(et)
}

// 根据执行时间推算得到定时任务从属的slot位置
func (t *TimeWheel) getPosAndCircle(executeAt time.Time) (int, int) {
	delay := int(time.Until(executeAt)) // 计算延迟时间
	// 定时任务的延迟轮次 延迟时间 / (槽位数*时间间隔)
	cycle := delay / (len(t.slots) * int(t.interval))
	// 定时任务从属的环状数组index
	pos := (t.curSlot + delay/int(t.interval)) % len(t.slots)
	return pos, cycle
}

func (t *TimeWheel) tick() {
	taskList := t.slots[t.curSlot] // 取出当前list
	defer t.circularIncr()         // 推进当前指针
	t.execute(taskList)            // 批量处理满足执行条件的定时任务
}

func (t *TimeWheel) execute(l *list.List) {
	for e := l.Front(); e != nil; {
		// 获取到每个节点对应的定时任务信息
		elem, _ := e.Value.(taskElement)
		if elem.cycle > 0 {
			elem.cycle-- // 轮次递减
			e = e.Next()
			continue
		}

		go func() {
			defer func() {
				if err := recover(); err != nil {
					// ....
				}
			}()
			elem.task() // 执行
		}()

		next := e.Next()
		l.Remove(e)

		// 将任务key从映射map中删除
		delete(t.keyToETask, elem.key)
		e = next
	}
}

func (t *TimeWheel) circularIncr() {
	t.curSlot = (t.curSlot + 1) % len(t.slots)
}

func (t *TimeWheel) Stop() {
	t.Do(func() {
		t.ticker.Stop()
		close(t.stopCh)
	})
}
