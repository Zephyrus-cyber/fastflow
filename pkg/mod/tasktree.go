package mod

import (
	"errors"
	"fmt"

	"github.com/etherealiy/fastflow/pkg/entity"
)

const (
	virtualTaskRootID = "_virtual_root"
	TaskEndID         = "End"
)

// TaskInfoGetter taskNode和taskIns均实现了该接口
type TaskInfoGetter interface {
	GetDepend() []string
	GetID() string
	GetGraphID() string
	GetStatus() entity.TaskInstanceStatus
}

// MapTaskInsToGetter
func MapTaskInsToGetter(taskIns []*entity.TaskInstance) (ret []TaskInfoGetter) {
	for i := range taskIns {
		ret = append(ret, taskIns[i])
	}
	return
}

// MapTasksToGetter
func MapTasksToGetter(taskIns []entity.Task) (ret []TaskInfoGetter) {
	for i := range taskIns {
		ret = append(ret, &taskIns[i])
	}
	return
}

// MapMockTasksToGetter
func MapMockTasksToGetter(taskIns []*MockTaskInfoGetter) (ret []TaskInfoGetter) {
	for i := range taskIns {
		ret = append(ret, taskIns[i])
	}
	return
}

// MustBuildRootNode
func MustBuildRootNode(tasks []TaskInfoGetter) *TaskNode {
	root, err := BuildRootNode(tasks)
	if err != nil {
		panic(fmt.Errorf("build tasks failed: %s", err))
	}
	return root
}

// BuildRootNode
func BuildRootNode(tasks []TaskInfoGetter) (*TaskNode, error) {
	root := &TaskNode{
		TaskInsID: virtualTaskRootID,
		Status:    entity.TaskInstanceStatusSuccess,
	}
	m, err := buildGraphNodeMap(tasks)
	if err != nil {
		return nil, err
	}

	for i := range tasks {
		// 入度为0的节点
		if len(tasks[i].GetDepend()) == 0 {
			n := m[tasks[i].GetGraphID()]
			n.AppendParent(root)
			root.children = append(root.children, n)
		}

		// 根据depend on构造每个节点的parent和child
		if len(tasks[i].GetDepend()) > 0 {
			for _, dependId := range tasks[i].GetDepend() {
				parent, ok := m[dependId]
				if !ok {
					return nil, fmt.Errorf("does not find task[%s] depend: %s", tasks[i].GetGraphID(), dependId)
				}
				parent.AppendChild(m[tasks[i].GetGraphID()])
				m[tasks[i].GetGraphID()].AppendParent(parent)
			}
		}
	}

	if len(root.children) == 0 {
		return nil, errors.New("here is no start nodes")
	}

	// FIXME： 当图规模较大时，检测环会一直卡住
	//if cycleStart := root.HasCycle(); cycleStart != nil {
	//	return nil, fmt.Errorf("dag has cycle at: %s", cycleStart.TaskInsID)
	//}

	return root, nil
}

// 返回的map，key是taskID，val.TaskInsID是_id（对应mongodb）
func buildGraphNodeMap(tasks []TaskInfoGetter) (map[string]*TaskNode, error) {
	m := map[string]*TaskNode{}
	for i := range tasks {
		if _, ok := m[tasks[i].GetGraphID()]; ok {
			return nil, fmt.Errorf("task id is repeat, id: %s", tasks[i].GetGraphID())
		}
		m[tasks[i].GetGraphID()] = NewTaskNodeFromGetter(tasks[i])
	}
	return m, nil
}

// TaskTree
type TaskTree struct {
	DagIns *entity.DagInstance
	Root   *TaskNode
}

// NewTaskNodeFromGetter
func NewTaskNodeFromGetter(instance TaskInfoGetter) *TaskNode {
	return &TaskNode{
		TaskInsID: instance.GetID(),
		Status:    instance.GetStatus(),
	}
}

// TaskNode
type TaskNode struct {
	TaskInsID string
	Status    entity.TaskInstanceStatus

	children []*TaskNode
	parents  []*TaskNode
}

type TreeStatus string

const (
	TreeStatusRunning TreeStatus = "running"
	TreeStatusSuccess TreeStatus = "success"
	TreeStatusFailed  TreeStatus = "failed"
	TreeStatusBlocked TreeStatus = "blocked"
)

// HasCycle
func (t *TaskNode) HasCycle() (cycleStart *TaskNode) {
	visited, incomplete := map[string]struct{}{}, map[string]*TaskNode{}
	waitQueue := []*TaskNode{t}
	bfsCheckCycle(waitQueue, visited, incomplete)
	if len(incomplete) > 0 {
		for k := range incomplete {
			return incomplete[k]
		}
	}
	return
}

func bfsCheckCycle(waitQueue []*TaskNode, visited map[string]struct{}, incomplete map[string]*TaskNode) {
	queueLen := len(waitQueue)
	if queueLen == 0 {
		return
	}

	isParentCompleted := func(node *TaskNode) bool {
		for _, p := range node.parents {
			if _, ok := visited[p.TaskInsID]; !ok {
				return false
			}
		}
		return true
	}

	for i := 0; i < queueLen; i++ {
		cur := waitQueue[i]
		if !isParentCompleted(cur) {
			incomplete[cur.TaskInsID] = cur
			continue
		}
		visited[cur.TaskInsID] = struct{}{}
		delete(incomplete, cur.TaskInsID)
		for _, c := range cur.children {
			waitQueue = append(waitQueue, c)
		}
	}
	waitQueue = waitQueue[queueLen:]
	bfsCheckCycle(waitQueue, visited, incomplete)
	return
}

// ComputeStatus
func (t *TaskNode) ComputeStatus() (status TreeStatus, srcTaskInsId string) {
	// 先判断是否所有节点的状态都为成功，避免图较大时需要进行dfs
	// 注意t是虚拟Root节点
	taskIns, err := GetStore().GetTaskIns(t.children[0].TaskInsID)
	if err == nil {
		dagInsId := taskIns.DagInsID
		endTask, err := GetStore().ListTaskInstance(&ListTaskInstanceInput{
			DagInsID: dagInsId,
			TaskID:   TaskEndID,
		})
		if err == nil && len(endTask) == 1 {
			if endTask[0].Status == entity.TaskInstanceStatusSuccess {
				return TreeStatusSuccess, ""
			}
		}
	}
	walkNode(t, func(node *TaskNode) bool {
		switch node.Status {
		case entity.TaskInstanceStatusFailed, entity.TaskInstanceStatusCanceled:
			status = TreeStatusFailed
			srcTaskInsId = node.TaskInsID
			return true
		case entity.TaskInstanceStatusBlocked:
			status = TreeStatusBlocked
			srcTaskInsId = node.TaskInsID
			return true
		case entity.TaskInstanceStatusSuccess, entity.TaskInstanceStatusSkipped:
			return true
		default:
			status = TreeStatusRunning
			srcTaskInsId = node.TaskInsID
			return false
		}
	}, false)
	if srcTaskInsId != "" {
		return
	}
	return TreeStatusSuccess, ""
}

func walkNode(root *TaskNode, walkFunc func(node *TaskNode) bool, walkChildrenIgnoreStatus bool) {
	dfsWalk(root, walkFunc, walkChildrenIgnoreStatus)
}

func dfsWalk(
	root *TaskNode,
	walkFunc func(node *TaskNode) bool,
	walkChildrenIgnoreStatus bool) bool {

	// 除了虚拟根节点外，都对该节点执行walkFunc
	// 当parser初始化dagIns时，此时的walkFunc是把可执行的节点添加进可执行列表中，并返回true
	//if len(root.children) == 0 && root.Status == entity.TaskInstanceStatusSuccess {
	//	fmt.Println()
	//}
	if root.TaskInsID != virtualTaskRootID {
		if !walkFunc(root) {
			return false
		}
	}

	// we cannot execute children, but should execute brother nodes
	// parser初始化dagIns时，walkChildrenIgnoreStatus为false，且虚拟根节点的状态为success，可以执行children
	if !walkChildrenIgnoreStatus && !root.CanExecuteChild() {
		return true
	}
	// parser初始化dagIns时，此时的children是图中入度为0的节点，对这些节点继续递归进行dfsWalk，由于虚拟根节点的状态为success，则把节点添加进可执行列表中，此时当前节点还未完成，则返回true，检查兄弟节点
	for _, c := range root.children {
		// if children's parent is not just root, we must check it
		if len(c.parents) > 1 && !c.CanBeExecuted() {
			continue
		}

		if !dfsWalk(c, walkFunc, walkChildrenIgnoreStatus) {
			return false
		}
	}
	return true
}

// AppendChild
func (t *TaskNode) AppendChild(task *TaskNode) {
	t.children = append(t.children, task)
}

// AppendParent
func (t *TaskNode) AppendParent(task *TaskNode) {
	t.parents = append(t.parents, task)
}

// CanExecuteChild
func (t *TaskNode) CanExecuteChild() bool {
	return t.Status == entity.TaskInstanceStatusSuccess || t.Status == entity.TaskInstanceStatusSkipped
}

// CanBeExecuted check whether task could be executed
func (t *TaskNode) CanBeExecuted() bool {
	if len(t.parents) == 0 {
		return true
	}

	for _, p := range t.parents {
		if !p.CanExecuteChild() {
			return false
		}
	}
	return true
}

// GetExecutableTaskIds is unique task id map
func (t *TaskNode) GetExecutableTaskIds() (executables []string) {
	// 从当前节点开始dfs遍历，且每个节点都执行函数（若节点可执行（所有parent节点已完成），则添加进执行列表中）
	walkNode(t, func(node *TaskNode) bool {
		if node.Executable() {
			executables = append(executables, node.TaskInsID)
		}
		return true
	}, false)
	return
}

// GetNextTaskIds 在该函数中会同步taskIns的状态到taskTree中，并寻找下一批可以执行的节点
func (t *TaskNode) GetNextTaskIds(completedOrRetryTask *entity.TaskInstance) (executable []string, find bool) {
	// walkFunc：从根节点开始walk
	// （1）如果walk到被寻找节点则标记find为true，并更新该节点的状态为对应taskIns的状态
	// 如果该taskIns的状态为Init，则将该节点加入可执行队列，并返回
	// 如果该taskIns还不可执行child（其状态不是success或者skip），返回
	// 否则，该taskIns执行success或被skip，可以执行children，若其children可执行（可能由多个parent，需要所有parent执行完成），则将该child加入可执行队列。将所有可执行child加入后，返回即可，不需要再dfs
	// （2）walk到的不是被寻找的节点，返回true，继续dfs遍历寻找
	walkNode(t, func(node *TaskNode) bool {
		if completedOrRetryTask.ID == node.TaskInsID {
			find = true
			node.Status = completedOrRetryTask.Status

			if node.Status == entity.TaskInstanceStatusInit {
				executable = append(executable, node.TaskInsID)
				return false
			}

			if !node.CanExecuteChild() {
				return false
			}
			for i := range node.children {
				if node.children[i].Executable() {
					executable = append(executable, node.children[i].TaskInsID)
				}
			}
			return false
		}
		return true
	}, false)

	return
}

// Executable
func (t *TaskNode) Executable() bool {
	if t.Status == entity.TaskInstanceStatusInit ||
		t.Status == entity.TaskInstanceStatusRetrying ||
		t.Status == entity.TaskInstanceStatusContinue ||
		t.Status == entity.TaskInstanceStatusEnding {
		if len(t.parents) == 0 {
			return true
		}

		for i := range t.parents {
			if !t.parents[i].CanExecuteChild() {
				return false
			}
		}
		return true
	}
	return false
}
