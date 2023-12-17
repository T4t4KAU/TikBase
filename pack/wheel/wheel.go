package wheel

import (
	"container/list"
	"sync"
	"time"
)

type taskElement struct {
	task  func()
	pos   int
	cycle int
	key   string
}

// TimeWheel 时间轮实现
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

func NewTimeWheel(slotNum int, interval time.Duration) *TimeWheel {
	if slotNum <= 0 {
		slotNum = 10
	}
	if interval <= 0 {
		interval = time.Second
	}

	t := TimeWheel{
		interval:     interval,
		ticker:       time.NewTicker(interval),
		stopCh:       make(chan struct{}),
		keyToETask:   make(map[string]*list.Element),
		slots:        make([]*list.List, 0),
		addTaskCh:    make(chan *taskElement),
		removeTaskCh: make(chan string),
	}

	for i := 0; i < slotNum; i++ {
		t.slots = append(t.slots, list.New())
	}

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

		}
	}
}

func (t *TimeWheel) AddTask(key string, task func(), executeAt time.Time) {
	pos, cycle := t.getPosAndCircle(executeAt)
	t.addTaskCh <- &taskElement{
		pos:   pos,
		cycle: cycle,
		task:  task,
		key:   key,
	}
}

func (t *TimeWheel) addTask(task *taskElement) {
	taskList := t.slots[task.pos]
	if _, ok := t.keyToETask[task.key]; ok {
		t.removeTask(task.key)
	}
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
	delay := int(time.Until(executeAt))
	// 定时任务的延迟轮次
	cycle := delay / (len(t.slots) * int(t.interval))
	// 定时任务从属的环状数组index
	pos := (t.curSlot + delay/int(t.interval)) % len(t.slots)
	return pos, cycle
}

func (t *TimeWheel) tick() {
	taskList := t.slots[t.curSlot]
	defer t.circularIncr()
	t.execute(taskList)
}

func (t *TimeWheel) execute(l *list.List) {
	for e := l.Front(); e != nil; {
		elem, _ := e.Value.(taskElement)
		if elem.cycle > 0 {
			elem.cycle--
			e = e.Next()
			continue
		}

		go func() {
			defer func() {
				if err := recover(); err != nil {
					// ....
				}
			}()
			elem.task()
		}()

		next := e.Next()
		l.Remove(e)
		delete(t.keyToETask, elem.key)
		e = next
	}
}

func (t *TimeWheel) circularIncr() {
	t.curSlot = (t.curSlot + 1) % len(t.slots)
}
