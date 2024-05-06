package doomdb

import (
	"reflect"
	"strings"
	"sync"
)

type cacheFieldsDB struct {
	structs map[string][]*fieldDB
	mu      *sync.RWMutex
}

func newCache() *cacheFieldsDB {
	return &cacheFieldsDB{
		structs: make(map[string][]*fieldDB, 20),
		mu:      &sync.RWMutex{},
	}
}

func (c *cacheFieldsDB) get(dest any) []*fieldDB {
	t := reflect.TypeOf(dest)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() == reflect.Slice {
		t = t.Elem()
		if t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
	}

	c.mu.RLock()
	fields, ok := c.structs[t.Name()]
	c.mu.RUnlock()
	if ok {
		return fields
	}

	fields = parseFields(t)

	c.mu.Lock()
	c.structs[t.Name()] = fields
	c.mu.Unlock()
	return fields
}

func (c *cacheFieldsDB) getSelectFields(dest any) string {
	fields := c.get(dest)
	res := make([]string, 0, len(fields))
	for _, field := range fields {
		res = append(res, field.toSelect())
	}

	return strings.Join(res, ", ")
}

func (c *cacheFieldsDB) getJoins(dest any) string {
	fields := c.get(dest)
	res := make([]string, 0, len(fields))
	for _, field := range fields {

		if field.join != "" {
			res = append(res, field.join)
		}
	}

	return strings.Join(res, " ")
}

func (c *cacheFieldsDB) getInsertFields(dest any) (string, string) {
	fields := c.get(dest)
	resFields := make([]string, 0, len(fields))
	resValues := make([]string, 0, len(fields))
	for _, field := range fields {
		resFields = append(resFields, field.toInsertName())
		resValues = append(resValues, field.toInsert())
	}

	return strings.Join(resFields, ", "), strings.Join(resValues, ", ")
}

func (c *cacheFieldsDB) getUpdateFields(dest any) string {
	fields := c.get(dest)
	res := make([]string, 0, len(fields))
	for _, field := range fields {
		res = append(res, field.toUpdate())
	}

	return strings.Join(res, ", ")
}
