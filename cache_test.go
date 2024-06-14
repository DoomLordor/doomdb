package doomdb

import (
	"strings"
	"testing"
)

type testCache struct {
	IntField    uint64 `db:"test.uint_field" default:"3" join:"LEFT JOIN test3 on test3.id=test.join"`
	StringField string `db:"test.string_field" join:"LEFT JOIN test2 on test2.id=test.join"`
}

func TestNewCache(t *testing.T) {
	cache := newCache()
	if cache == nil {
		t.Fatal("cache nil")
	}
	if cache.mu == nil {
		t.Error("cache mutex nil")
	}
	if cache.structs == nil {
		t.Error("cache structs nil")
	}
}

func TestCache_getWithOutValue(t *testing.T) {
	cache := newCache()
	fields := cache.get(&testCache{}, false)
	if fields == nil {
		t.Error("cache get fields is nil")
	}

	if len(cache.structs) == 0 {
		t.Fatal("len structs in cache = 0")
	}

	if _, ok := cache.structs["testCache"]; !ok {
		t.Fatal("testCache in cache not found")
	}
}

func TestCache_getWithValue(t *testing.T) {
	cache := newCache()
	fields := cache.get(&testCache{}, true)
	if fields == nil {
		t.Error("cache get fields is nil")
	}

	if len(cache.structs) != 0 {
		t.Fatal("len structs in cache != 0")
	}

	if _, ok := cache.structs["testCache"]; ok {
		t.Fatal("testCache found in cache")
	}
}

func TestCache_getSelectFields(t *testing.T) {
	cache := newCache()
	res := cache.getSelectFields(&testCache{})
	if strings.Join(res, " ") != `COALESCE(test.uint_field, '3') AS "test.uint_field" test.string_field AS "test.string_field"` {
		t.Error("get select field wrong")
	}
}

func TestCache_getJoins(t *testing.T) {
	cache := newCache()
	res := cache.getJoins(&testCache{})
	if strings.Join(res, " ") != `LEFT JOIN test3 on test3.id=test.join LEFT JOIN test2 on test2.id=test.join` {
		t.Error("get joins wrong")
	}
}

func TestCache_getInsertFields(t *testing.T) {
	cache := newCache()
	st := &testCache{}
	resName, resFields := cache.getInsertFields(st)

	if strings.Join(resName, ", ") != `test.uint_field, test.string_field` {
		t.Error("get insert fields names wrong")
	}

	if len(resFields) != 2 {
		t.Error("get insert fields wrong")
	}

	if resFields[0] != st.IntField {
		t.Error("get insert fields int wrong")
	}

	if resFields[1] != st.StringField {
		t.Error("get insert fields string wrong")
	}
}
