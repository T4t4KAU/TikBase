package txn

import (
	"errors"
	"fmt"
	"sync"
)

type registryCenter struct {
	mutex      sync.RWMutex
	components map[string]TccComponent
}

func newRegistryCenter() *registryCenter {
	return &registryCenter{
		components: make(map[string]TccComponent),
	}
}

func (r *registryCenter) register(com TccComponent) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, ok := r.components[com.ID()]; ok {
		return errors.New("repeat component id")
	}

	r.components[com.ID()] = com
	return nil
}

func (r *registryCenter) getComponents(componentIds ...string) ([]TccComponent, error) {
	components := make([]TccComponent, 0, len(componentIds))

	r.mutex.Lock()
	defer r.mutex.Unlock()

	for _, id := range componentIds {
		component, ok := r.components[id]
		if !ok {
			return nil, fmt.Errorf("component id: %s not existed", id)
		}
		components = append(components, component)
	}
	return components, nil
}
