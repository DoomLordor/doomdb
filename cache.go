package doomdb

import (
	"reflect"
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

func (c *cacheFieldsDB) get(dest any, withValue bool) []*fieldDB {
	v := reflect.ValueOf(dest)
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}

	if v.Kind() == reflect.Slice {
		if v.Len() == 0 {
			t := v.Type().Elem()
			if t.Kind() == reflect.Pointer {
				t = t.Elem()
			}
			v = reflect.New(t)
			if v.Kind() == reflect.Pointer {
				v = v.Elem()
			}
		} else {
			v = v.Index(0)
			if v.Kind() == reflect.Pointer {
				v = v.Elem()
			}
		}
	}

	if withValue {
		return parseFields(v)
	}
	t := v.Type()

	c.mu.RLock()
	fields, ok := c.structs[t.Name()]
	c.mu.RUnlock()
	if ok {
		return fields
	}

	fields = parseFields(v)

	c.mu.Lock()
	c.structs[t.Name()] = fields
	c.mu.Unlock()
	return fields
}

func (c *cacheFieldsDB) getSelectFields(dest any) []string {
	fields := c.get(dest, false)
	res := make([]string, 0, len(fields))
	for _, field := range fields {
		if field.name == "" {
			continue
		}
		res = append(res, field.toSelect())
	}

	return res
}

func (c *cacheFieldsDB) getJoins(dest any) []string {
	fields := c.get(dest, false)
	res := make([]string, 0, len(fields))
	for _, field := range fields {
		if field.join == "" {
			continue
		}
		res = append(res, field.join)
	}

	return res
}

func (c *cacheFieldsDB) getInsertFields(dest any) ([]string, []any) {
	fields := c.get(dest, true)
	resFields := make([]string, 0, len(fields))
	resValues := make([]any, 0, len(fields))
	for _, field := range fields {
		resFields = append(resFields, field.name)
		resValues = append(resValues, field.value)
	}

	return resFields, resValues
}
