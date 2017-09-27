package elasticsearchv5

import (
	"fmt"
	"strconv"
	"testing"
)

var a string

func TestSequence(t *testing.T) {
	_, s, err := getSequence(100)
	if err != nil {
		t.Error(err)
	}
	for i := 0; i < 200; i++ {
		a = s.GetID()
	}
}

func TestMaxID(t *testing.T) {
	client, s, err := getSequence(5)
	if err != nil {
		t.Error(err)
	}

	m := 10
	for i := 0; i <= m; i++ {
		_, err := client.Index(`{"some_field": "some_value"}`, strconv.Itoa(i))
		if err != nil {
			t.Error(err)
		}
	}

	if err != nil {
		t.Error(err)
	}
	id, err := s.getMaxID()
	if err != nil {
		t.Error(err)
	}
	// TODO: sometimes failes for async reasons
	if id <= 10 {
		t.Error("id must be at least 10 since 10 values were inserted here")
	}
}

func TestNewIndexWithExistingData(t *testing.T) {
	client, err := New("unit_test", "sequence_test2", "")
	if err != nil {
		t.Error(err)
	}

	m := 15
	for i := m; i <= 2*m; i++ {
		_, err := client.Index(`{"some_field": "some_value"}`, strconv.Itoa(i))
		if err != nil {
			t.Error(err)
		}
	}

	client2, err := New("sequence", "unit_test", "")
	if err != nil {
		fmt.Println(101)
		t.Error(err)
	}
	// ignore if not existent
	client2.Delete("sequence_test2")

	if err := client.SetSequenceAutoIncrement(1); err != nil {
		fmt.Println(103)
		t.Error(err)
	}

	id, err := strconv.Atoi(client.sequence.GetID())
	if err != nil {
		t.Error(err)
	}
	if id <= 2*m {
		t.Error("next id is already in use")
	}
}

func getSequence(cacheSize int) (*Elastic, *sequence, error) {
	client, err := New("unit_test", "sequence_test", "")
	if err != nil {
		return nil, nil, err
	}
	if err := client.SetSequenceAutoIncrement(cacheSize); err != nil {
		return nil, nil, err
	}
	return client, client.sequence, nil
}
